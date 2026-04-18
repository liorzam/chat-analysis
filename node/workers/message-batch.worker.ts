import { parentPort } from 'worker_threads';
import {
  aggregateMessageLines,
  type LookupTables,
  type PartialStats,
} from '../lib/aggregateBatch.js';

type InitMsg = { type: 'init'; lookups: LookupTables };
type RunMsg = { type: 'run'; id: number; lines: string[] };

function assertPort(): NonNullable<typeof parentPort> {
  if (!parentPort) throw new Error('This module must run in a worker thread');
  return parentPort;
}

const port = assertPort();

let lookups: LookupTables | null = null;

port.on('message', (msg: InitMsg | RunMsg) => {
  if (msg.type === 'init') {
    lookups = msg.lookups;
    port.postMessage({ type: 'ready' });
    return;
  }
  if (msg.type === 'run') {
    if (!lookups) {
      port.postMessage({ type: 'error', id: msg.id, err: 'Worker not initialized' });
      return;
    }
    let result: PartialStats;
    try {
      result = aggregateMessageLines(msg.lines, lookups);
    } catch (e) {
      const err = e instanceof Error ? e.message : String(e);
      port.postMessage({ type: 'error', id: msg.id, err });
      return;
    }
    port.postMessage({ type: 'done', id: msg.id, result }, []);
  }
});

