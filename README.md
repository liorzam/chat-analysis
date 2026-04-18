# python-homeassesment

This repository contains two implementations that compute the same **chat analytics** over CSV inputs (`messages.csv`, `users.csv`, `channel_participants.csv`):

- **`python/`** — Flask API plus a CLI that streams messages in pandas chunks and writes `data/analytics_results.json`; see [`python/README.md`](python/README.md) for setup, CLI/API usage, and file formats.
- **`node/`** — TypeScript CLI that streams line batches and can aggregate with a worker pool; see [`node/README.md`](node/README.md) for flags and file formats.

The implementations share the same four headline metrics (ties broken consistently).

## Benchmark: what we compare

Each run answers:

| # | Metric |
|---|--------|
| 1 | User who **sent** the most messages |
| 2 | User who **received** the most messages |
| 3 | Organization that **sent** the most messages |
| 4 | Organization that **received** the most messages |

**Timing** is wall-clock for a full aggregation pass (after small setup loads). Absolute numbers depend on CPU, disk, and whether outputs are already cached—use the tables below as a **sample** from one dataset and machine, not a universal ranking.

## Sample results (same leaderboard)

On a single large run, both stacks agreed on the four winners:

| Metric | Value |
|--------|--------|
| Top sender (user) | `user_1027` (8000) |
| Top receiver (user) | `user_2080` (247381) |
| Top sender (org) | `org_55` (466979) |
| Top receiver (org) | `org_64` (6682830) |

## Sample timings

| Stack | Command (abbrev.) | Notable phases | Wall time |
|--------|-------------------|----------------|----------|
| **Python** | `python -m chat.cli` from `python/` (computes when cache invalid) | **`MESSAGES_CHUNK_SIZE` = 25_000** (matches Node `batchLines`) | **~6.589s** |
| **Node (workers)** | `tsc && node dist/main.js build --workers` from `node/` | `mode=workers`, workers=6, batchLines=25000, highWaterMark=2097152 | **~7345 ms** |
| **Node (sequential)** | `tsc && node dist/main.js build --sequential` | `mode=sequential` | **~15845 ms** |

With **25k rows per chunk**, that Python sample and the **Node workers** sample above are in the same ballpark (**~6.6s vs ~7.3s** wall time on the runs captured here—different heaps and parsers, so do not over-read a few hundred milliseconds).

On the same inputs, **worker mode is much faster than sequential** Node here (~2.2× end-to-end vs `--sequential`). The four headline numbers match either way; only wall time changes.

**Tuning:** You can often squeeze out better Node throughput by changing **`--batch-lines`** (how many complete lines are handed off per chunk) and **`--high-water-mark`** (read stream buffer size, default 2 MiB). Larger batches can reduce coordination overhead; larger buffers can help fast disks—too large can hurt latency or memory. There is no single best pair for every machine and CSV; it is worth experimenting if you care about last-mile performance. On the Python side, the matching knob is **`MESSAGES_CHUNK_SIZE`** in `python/chat/chat_analytics.py` (default 25_000, aligned with Node for fair comparison).


## How to reproduce

1. Point both tools at the **same** `data/` directory (see each subproject’s config: Python `settings`/`.env`, Node `--messages`, `--users`, `--participants`, `--out`).
2. **Python** (with deps installed, from `python/`):

   ```bash
   python -m chat.cli
   ```

   To force recomputation, remove or rename `data/analytics_results.json` first (the CLI uses it as a cache when valid).

3. **Node** (from `node/`):

   ```bash
   pnpm install
   pnpm run analyze-workers
   ```

   Or explicitly: `tsc && node dist/main.js build --workers` (add path flags if your CSVs are outside `./data`).

For deeper Node options (`--sequential`, `--batch-lines`, etc.), see [`node/README.md`](node/README.md).
