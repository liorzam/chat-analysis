/**
 * Pure aggregation of message CSV lines into additive counter records (monoid-friendly).
 */

export type LookupTables = {
  userOrg: Record<string, string>;
  /** channel_id -> participant user_ids */
  channels: Record<string, string[]>;
};

export type PartialStats = {
  userSent: Record<string, number>;
  userReceived: Record<string, number>;
  orgSent: Record<string, number>;
  orgReceived: Record<string, number>;
  skippedRows: number;
};

export const emptyPartialStats = (): PartialStats => ({
  userSent: {},
  userReceived: {},
  orgSent: {},
  orgReceived: {},
  skippedRows: 0,
});

export function mergeNumericRecords(a: Record<string, number>, b: Record<string, number>): Record<string, number> {
  if (Object.keys(b).length === 0) return { ...a };
  if (Object.keys(a).length === 0) return { ...b };
  const out: Record<string, number> = { ...a };
  for (const [k, v] of Object.entries(b)) {
    out[k] = (out[k] ?? 0) + v;
  }
  return out;
}

export function mergePartialStats(a: PartialStats, b: PartialStats): PartialStats {
  return {
    userSent: mergeNumericRecords(a.userSent, b.userSent),
    userReceived: mergeNumericRecords(a.userReceived, b.userReceived),
    orgSent: mergeNumericRecords(a.orgSent, b.orgSent),
    orgReceived: mergeNumericRecords(a.orgReceived, b.orgReceived),
    skippedRows: a.skippedRows + b.skippedRows,
  };
}

export function mergeManyPartialStats(parts: readonly PartialStats[]): PartialStats {
  if (parts.length === 0) return emptyPartialStats();
  return parts.reduce(mergePartialStats);
}

function bump(r: Record<string, number>, id: string, delta: number) {
  r[id] = (r[id] ?? 0) + delta;
}

/** Aggregate one batch of data lines (no CSV header). Logs warnings via optional callback per skipped row. */
export function aggregateMessageLines(
  lines: readonly string[],
  lookups: LookupTables,
  onSkip?: (reason: string, preview: string) => void
): PartialStats {
  const userSent: Record<string, number> = {};
  const userReceived: Record<string, number> = {};
  const orgSent: Record<string, number> = {};
  const orgReceived: Record<string, number> = {};
  let skippedRows = 0;

  const { userOrg, channels } = lookups;

  for (let i = 0; i < lines.length; i++) {
    const line = lines[i];
    const parts = line.split(',');
    if (parts.length < 3) {
      skippedRows++;
      onSkip?.('malformed', line.slice(0, 80));
      continue;
    }
    const channelId = parts[1].trim();
    const senderId = parts[2].trim();
    if (!senderId) {
      skippedRows++;
      continue;
    }
    const participantList = channels[channelId];
    if (!participantList) {
      skippedRows++;
      onSkip?.(`unknown channel ${channelId}`, line.slice(0, 80));
      continue;
    }

    bump(userSent, senderId, 1);
    const senderOrg = userOrg[senderId];
    if (senderOrg) bump(orgSent, senderOrg, 1);

    for (const uid of participantList) {
      if (uid === senderId) continue;
      bump(userReceived, uid, 1);
      const org = userOrg[uid];
      if (org) bump(orgReceived, org, 1);
    }
  }

  return { userSent, userReceived, orgSent, orgReceived, skippedRows };
}

export function mapToUserCountsRecord(userSent: Record<string, number>, userReceived: Record<string, number>) {
  const ids = new Set([...Object.keys(userSent), ...Object.keys(userReceived)]);
  const out: Record<string, { sent: number; received: number }> = {};
  for (const id of ids) {
    out[id] = {
      sent: userSent[id] ?? 0,
      received: userReceived[id] ?? 0,
    };
  }
  return out;
}

export function mergeOrgRecords(orgSent: Record<string, number>, orgReceived: Record<string, number>) {
  const ids = new Set([...Object.keys(orgSent), ...Object.keys(orgReceived)]);
  const out: Record<string, { sent: number; received: number }> = {};
  for (const id of ids) {
    out[id] = {
      sent: orgSent[id] ?? 0,
      received: orgReceived[id] ?? 0,
    };
  }
  return out;
}

/** Build serializable lookups from main-thread Maps. */
export function toLookupTables(
  userOrg: Map<string, string>,
  channelParticipants: Map<string, Set<string>>
): LookupTables {
  const uo: Record<string, string> = {};
  for (const [k, v] of userOrg) uo[k] = v;
  const ch: Record<string, string[]> = {};
  for (const [cid, set] of channelParticipants) {
    ch[cid] = [...set];
  }
  return { userOrg: uo, channels: ch };
}
