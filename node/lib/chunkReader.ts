import { createReadStream } from 'fs';

export type ReadLineBatchesOptions = {
  /** Stream buffer size (default 4 MiB). */
  highWaterMark?: number;
  /** Max complete lines per yielded batch (after header is skipped). */
  batchLines: number;
};

/**
 * Async generator: reads a file in chunks, splits on newlines, skips the first line (CSV header).
 * Handles CRLF and a final line without trailing newline.
 */
export async function* readLineBatches(
  filePath: string,
  options: ReadLineBatchesOptions
): AsyncGenerator<readonly string[]> {
  const { highWaterMark = 4 * 1024 * 1024, batchLines } = options;
  const stream = createReadStream(filePath, { highWaterMark });

  let carry = '';
  let headerSkipped = false;
  let batch: string[] = [];

  for await (const chunk of stream) {
    const s = carry + chunk.toString('utf8');
    const parts = s.split('\n');
    carry = parts.pop() ?? '';

    for (const part of parts) {
      const line = part.replace(/\r$/, '');
      if (!headerSkipped) {
        headerSkipped = true;
        continue;
      }
      batch.push(line);
      if (batch.length >= batchLines) {
        yield batch;
        batch = [];
      }
    }
  }

  if (carry.length > 0) {
    const line = carry.replace(/\r$/, '');
    if (!headerSkipped) {
      headerSkipped = true;
    } else {
      batch.push(line);
      if (batch.length >= batchLines) {
        yield batch;
        batch = [];
      }
    }
  }

  if (batch.length > 0) {
    yield batch;
  }
}
