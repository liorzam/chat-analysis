import { existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { Worker } from 'node:worker_threads';
import type { LookupTables, PartialStats } from './aggregateBatch.js';

/** Walk up from this file until we find package.json (stable whether running from lib/ or dist/lib/). */
function packageRootFrom(moduleUrl: string): string {
  let dir = dirname(fileURLToPath(moduleUrl));
  for (;;) {
    if (existsSync(join(dir, 'package.json'))) return dir;
    const parent = dirname(dir);
    if (parent === dir) throw new Error('Could not find package.json (needed to resolve worker path)');
    dir = parent;
  }
}

type WorkerOutbound =
  | { type: 'ready' }
  | { type: 'done'; id: number; result: PartialStats }
  | { type: 'error'; id: number; err: string };

export type MessageWorkerRunner = {
  run: (lines: readonly string[]) => Promise<PartialStats>;
  close: () => Promise<void>;
};

export async function createMessageWorkerRunner(
  lookups: LookupTables,
  workerCount: number
): Promise<MessageWorkerRunner> {
  const workerPath = join(packageRootFrom(import.meta.url), 'dist/workers/message-batch.worker.js');
  if (!existsSync(workerPath)) {
    throw new Error(`Worker bundle missing: ${workerPath}\nRun pnpm run build (or npm run build) before analyze.`);
  }

  const workers: Worker[] = [];
  const pending = new Map<number, { resolve: (v: PartialStats) => void; reject: (e: Error) => void }>();
  let nextId = 0;
  let rr = 0;

  for (let i = 0; i < workerCount; i++) {
    const w = new Worker(workerPath);
    workers.push(w);

    w.on('message', (msg: WorkerOutbound) => {
      if (msg.type === 'done') {
        const c = pending.get(msg.id);
        pending.delete(msg.id);
        c?.resolve(msg.result);
      } else if (msg.type === 'error') {
        const c = pending.get(msg.id);
        pending.delete(msg.id);
        c?.reject(new Error(msg.err));
      }
    });

    w.on('error', (err) => {
      for (const [, c] of pending) {
        c.reject(err);
      }
      pending.clear();
    });
  }

  await Promise.all(
    workers.map(
      (w) =>
        new Promise<void>((resolve, reject) => {
          const once = (msg: WorkerOutbound) => {
            if (msg.type === 'ready') {
              w.off('message', once);
              resolve();
            }
          };
          w.on('message', once);
          w.once('error', reject);
          w.postMessage({ type: 'init', lookups });
        })
    )
  );

  async function run(lines: readonly string[]): Promise<PartialStats> {
    const id = nextId++;
    const w = workers[rr % workers.length];
    rr++;
    return new Promise((resolve, reject) => {
      pending.set(id, { resolve, reject });
      w.postMessage({ type: 'run', id, lines: [...lines] });
    });
  }

  async function close() {
    await Promise.all(workers.map((w) => w.terminate()));
  }

  return { run, close };
}
