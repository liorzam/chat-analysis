import {
  aggregateMessageLines,
  emptyPartialStats,
  mergePartialStats,
  type LookupTables,
  type PartialStats,
} from './aggregateBatch.js';
import { readLineBatches } from './chunkReader.js';

/**
 * Fold streamed line batches on the main thread: same monoid semantics as the worker pool path.
 */
export async function aggregateMessagesSequential(
  messagesPath: string,
  batchLines: number,
  highWaterMark: number,
  lookups: LookupTables
): Promise<PartialStats> {
  let acc = emptyPartialStats();
  for await (const batch of readLineBatches(messagesPath, { batchLines, highWaterMark })) {
    acc = mergePartialStats(acc, aggregateMessageLines(batch, lookups));
  }
  return acc;
}
