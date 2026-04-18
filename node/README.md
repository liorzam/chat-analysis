# chat-analysis

A small Node.js CLI that **streams** chat message CSVs and aggregates **per-user** and **per-organization** send/receive counts. It writes a JSON snapshot you can re-report without re-reading the message file.

Designed for large `messages.csv` files: data is read in batches with a configurable read buffer, and aggregation can run on the main thread or across **worker threads** (default).

## What it measures

For each message row:

- The **sender** gets +1 **sent** (user and org, if the user has an org).
- Every **other channel participant** gets +1 **received** (user and org).

Channels come from `channel_participants.csv`. Users are mapped to orgs via `users.csv`. Rows with unknown channels or malformed lines are skipped and counted.

After aggregation, the tool prints four leaderboard lines (ties broken by **lexicographically smallest** id):

1. User who sent the most messages  
2. User who received the most messages  
3. Organization that sent the most messages  
4. Organization that received the most messages  

## Input CSV formats

All files use a **header row** (the first line is skipped). Parsing is simple comma-splitting (no quoted-field handling).

| File | Columns (conceptual) |
|------|------------------------|
| **messages.csv** | `…`, `channel_id`, `sender_id` — each data line must have **at least three** comma-separated fields; column 0 is unused for stats, column 1 is channel, column 2 is sender. |
| **users.csv** | `user_id`, `org_id` |
| **channel_participants.csv** | `channel_id`, `user_id` — one row per membership |

## Output (`chat-stats.json`)

A JSON object with:

- `version` — schema version (currently `1`)
- `generatedAt` — ISO 8601 timestamp
- `users` — map `user_id` → `{ sent, received }`
- `orgs` — map `org_id` → `{ sent, received }`

## Requirements

- **Node.js** (ES modules; project targets ES2022)
- **pnpm** is listed as the package manager (`packageManager` in `package.json`); npm/yarn work if you prefer.

## Setup

```bash
pnpm install
pnpm build
```

This runs `tsc` and emits JavaScript under `dist/`.

## Usage

### npm scripts

| Script | Action |
|--------|--------|
| `pnpm build` | Compile TypeScript only. |
| `pnpm analyze` | Build, then run **`build`** (aggregate from CSVs, write stats + print report). |
| `pnpm report` | Build, then run **`report`** (read existing stats JSON, print report only). |

### CLI entry

After `pnpm build`, you can run the compiled entry:

```bash
node dist/main.js [build|report] [options]
```

The `lior` bin (`./bin/lior.mjs`) **compiles the project** with the local TypeScript compiler, then spawns `node dist/main.js` with your arguments—useful when you do not want to run `pnpm build` manually.

```bash
pnpm exec lior build
pnpm exec lior report
```

If `build` is omitted, it defaults to **`build`**.

### Options (both `build` and `report` where paths apply)

| Flag | Default | Description |
|------|---------|-------------|
| `--messages <path>` | `./data/messages.csv` | Message CSV path. |
| `--users <path>` | `./data/users.csv` | User → org CSV. |
| `--participants <path>` | `./data/channel_participants.csv` | Channel participants CSV. |
| `--out <path>` | `./data/chat-stats.json` | Stats JSON output (`build`) or input (`report`). |
| `--workers <n>` | `min(6, os.availableParallelism())` | Concurrent worker batches (ignored with `--sequential`). |
| `--batch-lines <n>` | `25000` | Max complete lines per batch when streaming. |
| `--high-water-mark <bytes>` | `2097152` (2 MiB) | `fs.createReadStream` buffer size. |
| `--sequential` | off | Aggregate on the main thread only (no worker pool). |

## Project layout

| Path | Role |
|------|------|
| `main.ts` | CLI, CSV loaders, orchestration. |
| `lib/chunkReader.ts` | Streaming line batches with header skip. |
| `lib/aggregateBatch.ts` | Pure per-batch counters and merges. |
| `lib/aggregateMessagesSequential.ts` | Main-thread streaming fold. |
| `lib/messageWorkerPool.ts` | Worker pool for parallel batches. |
| `workers/message-batch.worker.ts` | Worker entry: aggregate a line batch. |
| `bin/lior.mjs` | On-the-fly compile + run. |

## License

ISC (see `package.json`).
