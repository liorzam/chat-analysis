import { readFile, writeFile } from 'node:fs/promises';
import os from 'os';
import { resolve } from 'path';
import {
  emptyPartialStats,
  mapToUserCountsRecord,
  mergeManyPartialStats,
  mergeOrgRecords,
  mergePartialStats,
  toLookupTables,
  type PartialStats,
} from './lib/aggregateBatch.js';
import { readLineBatches } from './lib/chunkReader.js';
import { aggregateMessagesSequential } from './lib/aggregateMessagesSequential.js';
import { createMessageWorkerRunner } from './lib/messageWorkerPool.js';

const SCHEMA_VERSION = 1;

type UserCounts = { sent: number; received: number };
type StatsFile = {
  version: number;
  generatedAt: string;
  users: Record<string, UserCounts>;
  orgs: Record<string, UserCounts>;
};

const defaults = {
  messages: './data/messages.csv',
  users: './data/users.csv',
  participants: './data/channel_participants.csv',
  out: './data/chat-stats.json',
  analyticsOut: './data/analytics_results.json',
  /** Ignored when sequential is true. */
  workers: Math.min(6, Math.max(1, os.availableParallelism())),
  batchLines: 25_000,
  highWaterMark: 2 * 1024 * 1024,
  /** Main-thread fold over streamed batches (no worker threads). */
  sequential: false,
};

type BuildOptions = typeof defaults;

function parseArgs(argv: string[]): BuildOptions {
  const args: BuildOptions = { ...defaults };
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--messages' && argv[i + 1]) args.messages = argv[++i];
    else if (a === '--users' && argv[i + 1]) args.users = argv[++i];
    else if (a === '--participants' && argv[i + 1]) args.participants = argv[++i];
    else if (a === '--out' && argv[i + 1]) args.out = argv[++i];
    else if (a === '--analytics-out' && argv[i + 1]) args.analyticsOut = argv[++i];
    else if (a === '--workers' && argv[i + 1]) args.workers = Math.max(1, parseInt(argv[++i], 10) || 1);
    else if (a === '--batch-lines' && argv[i + 1]) args.batchLines = Math.max(1, parseInt(argv[++i], 10) || 1);
    else if (a === '--high-water-mark' && argv[i + 1]) args.highWaterMark = Math.max(1024, parseInt(argv[++i], 10) || args.highWaterMark);
    else if (a === '--sequential') args.sequential = true;
  }
  return args;
}

/** Load user_id → org_id from users.csv (header row skipped). */
async function loadUserOrg(csvPath: string): Promise<Map<string, string>> {
  const raw = await readFile(csvPath, 'utf-8');
  const lines = raw.trim().split('\n');
  const map = new Map<string, string>();
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',');
    if (parts.length < 2) continue;
    const userId = parts[0].trim();
    const orgId = parts[1].trim();
    if (userId) map.set(userId, orgId);
  }
  return map;
}

/** Load channel_id → Set of user_id from channel_participants.csv. */
async function loadChannelParticipants(csvPath: string): Promise<Map<string, Set<string>>> {
  const raw = await readFile(csvPath, 'utf-8');
  const lines = raw.trim().split('\n');
  const map = new Map<string, Set<string>>();
  for (let i = 1; i < lines.length; i++) {
    const parts = lines[i].split(',');
    if (parts.length < 2) continue;
    const channelId = parts[0].trim();
    const userId = parts[1].trim();
    if (!channelId || !userId) continue;
    let set = map.get(channelId);
    if (!set) {
      set = new Set<string>();
      map.set(channelId, set);
    }
    set.add(userId);
  }
  return map;
}

/** Fold streaming batches with at most `poolChunks` parallel worker invocations (monoid merge). */
async function aggregateMessagesWithPool(
  messagesPath: string,
  poolChunks: number,
  batchLines: number,
  highWaterMark: number,
  run: (lines: readonly string[]) => Promise<PartialStats>
): Promise<PartialStats> {
  let acc = emptyPartialStats();
  const buf: string[][] = [];

  for await (const batch of readLineBatches(messagesPath, { batchLines, highWaterMark })) {
    buf.push([...batch]);
    if (buf.length >= poolChunks) {
      const parts = await Promise.all(buf.map((lines) => run(lines)));
      acc = mergePartialStats(acc, mergeManyPartialStats(parts));
      buf.length = 0;
    }
  }
  if (buf.length > 0) {
    const parts = await Promise.all(buf.map((lines) => run(lines)));
    acc = mergePartialStats(acc, mergeManyPartialStats(parts));
  }
  return acc;
}

type TopEntry = { id: string | null; count: number };

type AnalyticsResults = {
  generated_at: string;
  top_sender_user: TopEntry;
  top_receiver_user: TopEntry;
  top_sender_org: TopEntry;
  top_receiver_org: TopEntry;
};

/** ISO-8601 UTC with fractional seconds padded to microseconds (…Z), similar to Python `isoformat`. */
function analyticsTimestamp(): string {
  return new Date().toISOString().replace(/\.(\d{3})Z$/, (_, ms: string) => `.${ms}000Z`);
}

/** Tie-break: lexicographically smallest id among those with the maximum count. */
function pickMaxEntry(counts: Map<string, number>): TopEntry {
  let bestId: string | null = null;
  let bestN = -Infinity;
  for (const [id, n] of counts) {
    if (bestId === null || n > bestN || (n === bestN && id < bestId)) {
      bestId = id;
      bestN = n;
    }
  }
  if (bestId === null) return { id: null, count: 0 };
  return { id: bestId, count: bestN };
}

function statsCountMaps(data: StatsFile) {
  const userSent = new Map<string, number>();
  const userReceived = new Map<string, number>();
  for (const [uid, c] of Object.entries(data.users)) {
    userSent.set(uid, c.sent);
    userReceived.set(uid, c.received);
  }
  const orgSent = new Map<string, number>();
  const orgReceived = new Map<string, number>();
  for (const [oid, c] of Object.entries(data.orgs)) {
    orgSent.set(oid, c.sent);
    orgReceived.set(oid, c.received);
  }
  return { userSent, userReceived, orgSent, orgReceived };
}

function buildAnalyticsResults(data: StatsFile): AnalyticsResults {
  const { userSent, userReceived, orgSent, orgReceived } = statsCountMaps(data);
  return {
    generated_at: analyticsTimestamp(),
    top_sender_user: pickMaxEntry(userSent),
    top_receiver_user: pickMaxEntry(userReceived),
    top_sender_org: pickMaxEntry(orgSent),
    top_receiver_org: pickMaxEntry(orgReceived),
  };
}

async function writeAnalyticsFile(path: string, data: StatsFile): Promise<void> {
  const payload = buildAnalyticsResults(data);
  await writeFile(path, JSON.stringify(payload, null, 2), 'utf-8');
}

function printReportFromStats(data: StatsFile) {
  const { userSent, userReceived, orgSent, orgReceived } = statsCountMaps(data);

  const topSender = pickMaxEntry(userSent);
  const topReceiver = pickMaxEntry(userReceived);
  const topOrgSent = pickMaxEntry(orgSent);
  const topOrgReceived = pickMaxEntry(orgReceived);

  console.log('1. User who sent the most messages:', topSender.id ?? '(none)', topSender.id ? `(${topSender.count})` : '');
  console.log('2. User who received the most messages:', topReceiver.id ?? '(none)', topReceiver.id ? `(${topReceiver.count})` : '');
  console.log('3. Organization that sent the most messages:', topOrgSent.id ?? '(none)', topOrgSent.id ? `(${topOrgSent.count})` : '');
  console.log('4. Organization that received the most messages:', topOrgReceived.id ?? '(none)', topOrgReceived.id ? `(${topOrgReceived.count})` : '');
}

async function cmdBuild(argv: string[]) {
  const opt = parseArgs(argv);
  const messagesPath = resolve(opt.messages);
  const usersPath = resolve(opt.users);
  const participantsPath = resolve(opt.participants);
  const outPath = resolve(opt.out);
  const analyticsPath = resolve(opt.analyticsOut);

  const start = Date.now();
  console.error('Loading users…');
  const userOrg = await loadUserOrg(usersPath);
  console.error('Loading channel participants…');
  const channelParticipants = await loadChannelParticipants(participantsPath);

  const lookups = toLookupTables(userOrg, channelParticipants);

  let partial: PartialStats;

  if (opt.sequential) {
    console.error(`Streaming messages (mode=sequential, batchLines=${opt.batchLines}, highWaterMark=${opt.highWaterMark})…`);
    partial = await aggregateMessagesSequential(
      messagesPath,
      opt.batchLines,
      opt.highWaterMark,
      lookups
    );
  } else {
    console.error(`Streaming messages (mode=workers, workers=${opt.workers}, batchLines=${opt.batchLines})…`);
    const runner = await createMessageWorkerRunner(lookups, opt.workers);
    try {
      partial = await aggregateMessagesWithPool(
        messagesPath,
        opt.workers,
        opt.batchLines,
        opt.highWaterMark,
        (lines) => runner.run(lines)
      );
    } finally {
      await runner.close();
    }
  }

  if (partial.skippedRows) {
    console.error(`Skipped ${partial.skippedRows} message row(s).`);
  }

  const stats: StatsFile = {
    version: SCHEMA_VERSION,
    generatedAt: new Date().toISOString(),
    users: mapToUserCountsRecord(partial.userSent, partial.userReceived),
    orgs: mergeOrgRecords(partial.orgSent, partial.orgReceived),
  };

  await writeFile(outPath, JSON.stringify(stats, null, 2), 'utf-8');
  await writeAnalyticsFile(analyticsPath, stats);
  console.error(`Wrote ${outPath} (${Date.now() - start} ms)`);
  console.error(`Wrote ${analyticsPath}`);

  printReportFromStats(stats);
}

async function cmdReport(argv: string[]) {
  const paths = parseArgs(argv);
  const outPath = resolve(paths.out);
  const analyticsPath = resolve(paths.analyticsOut);
  const raw = await readFile(outPath, 'utf-8');
  const data = JSON.parse(raw) as StatsFile;
  if (data.version !== SCHEMA_VERSION) {
    console.error(`[warn] stats file version ${data.version}, expected ${SCHEMA_VERSION}`);
  }
  await writeAnalyticsFile(analyticsPath, data);
  console.error(`Wrote ${analyticsPath}`);
  printReportFromStats(data);
}

async function main() {
  const argv = process.argv.slice(2);
  let sub: 'build' | 'report' = 'build';
  let rest = argv;
  if (argv[0] === 'report' || argv[0] === 'build') {
    sub = argv[0];
    rest = argv.slice(1);
  }
  if (sub === 'report') {
    await cmdReport(rest);
    return;
  }
  await cmdBuild(rest);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
