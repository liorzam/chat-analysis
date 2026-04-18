# chat-analysis

Flask service and offline pipeline for **chat analytics** over CSV inputs: top senders/receivers by **user** and by **organization**, with chunked processing so large `messages.csv` files stay memory-bounded.

This repository is structured for **multiple language implementations**: shared inputs live under `data/` at the repository root. See the [repository README](../README.md) for the Node stack and a cross-implementation benchmark.

## What it does

- **Sends** come from `messages.csv` (one row = one message from `user_id` in `channel_id`).
- **Receives** are derived: a message in channel *C* is counted as received by every *other* participant of *C* (see [python/chat/question.md](python/chat/question.md) for the full problem statement).
- **Organizations** are joined via `users.csv`; org totals are sums of member user totals.
- Results are validated with **Pydantic** and optionally **cached** at `data/analytics_results.json` so repeated runs (or API calls) skip recomputation when the file is valid.

## Python implementation

Layout: `pyproject.toml`, `uv.lock`, `app.py`, `settings.py`, and sources under `chat/` (CLI, analytics, schemas).

### Requirements

- Python **3.12+**
- [uv](https://docs.astral.sh/uv/) recommended (lockfile: `python/uv.lock`)

### Setup

From the repository root:

```bash
uv sync --project python
cp .env.example .env
```

- **`uv sync --project python`** — Reads `python/pyproject.toml` and `python/uv.lock`, creates `python/.venv`, and installs dependencies.
- **`cp .env.example .env`** — Optional. Copies the template at the repo root; edit `.env` to override defaults (see [Configuration](#configuration)). Skip if you are fine with defaults from `python/settings.py`.

Place the input CSVs under `data/` at the **repository root**:

| File | Role |
|------|------|
| `messages.csv` | `message_id`, `channel_id`, `user_id`, `timestamp` |
| `users.csv` | `user_id`, `org_id`, `username` |
| `channel_participants.csv` | `channel_id`, `user_id` (roster per channel) |

**Large dataset:** The repo may include a small sample `messages.csv`. For full-scale runs, replace `data/messages.csv` with your large file, keeping the same columns (`message_id`, `channel_id`, `user_id`, `timestamp`). Ensure `users.csv` and `channel_participants.csv` match that scenario.

Optional output (created or read by the app):

- `data/analytics_results.json` — cached headline metrics (see schema below).

### Configuration

Environment variables load from `.env` at the **repository root** or next to `python/settings.py`. Defaults are defined in [python/settings.py](python/settings.py).

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `5000` | HTTP port |
| `HOST` / bind | `127.0.0.1` | Listen address (`host` in settings) |
| `FLASK_DEBUG` | `false` | `1` / `true` / `yes` / `on` enable Flask debug |
| `DATA_DIR` | `data` | Path to CSV inputs and `analytics_results.json` (relative paths resolve from the **repository root**) |

See [.env.example](.env.example) for a template.

### Run the API

Complete flow from the repository root (after [Setup](#setup) and with CSVs under `data/`):

| Step | Command | What it does |
|------|---------|--------------|
| 1 | `uv sync --project python` | Installs locked dependencies (only needed when setting up or after lockfile changes). |
| 2 | `cp .env.example .env` | Optional: tune `HOST` / `PORT` / `FLASK_DEBUG` (see [Configuration](#configuration)). |
| 3 | *(data)* | Put `messages.csv`, `users.csv`, and `channel_participants.csv` in `data/`. Use your **large** `messages.csv` instead of the sample if you are exercising the full dataset. |
| 4 | `uv run --project python python app.py` | Starts the Flask app (default bind `127.0.0.1:5000` unless overridden in `.env`). Working directory must be `python/` so `app` resolves; the command below uses `-C`. |
| 5 | `curl` to `/api/chat/analytics` (example below pipes to `jq`) | Requests analytics JSON; `curl -s` hides progress; `jq` formats JSON (optional). |
| 6 | `uv run --project python -C python gunicorn -b 127.0.0.1:5000 app:app` | **Alternative** to step 4: production-style HTTP server. |

Minimal copy-paste sequence for local dev:

```bash
uv sync --project python
cp .env.example .env   # optional
uv run --project python -C python python app.py
```

In another terminal, call the API:

```bash
curl -s http://127.0.0.1:5000/api/chat/analytics | jq
```

If you changed `PORT` or `HOST` in `.env`, use that host/port in the URL instead of `127.0.0.1:5000`.

**Endpoints:**

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/` | Health-style JSON: `{ "status": "ok", ... }` |
| `GET` | `/api/chat/analytics` | Chat analytics JSON + `meta` (`source`: `cache` or `computed`; `perf` when freshly computed) |

**Response shape:** `analytics` matches [ChatAnalyticsResult](python/chat/analytics_schemas.py): `top_sender_user`, `top_receiver_user`, `top_sender_org`, `top_receiver_org` (each `{ "id", "count" }`), plus `generated_at`.

### CLI (offline analytics)

Prints the same headline metrics and either reads `data/analytics_results.json` or computes and writes it:

```bash
uv run --project python -C python python -m chat.cli
```

### Implementation notes

- **Chunked reads** of `messages.csv` (`MESSAGES_CHUNK_SIZE` in [python/chat/chat_analytics.py](python/chat/chat_analytics.py)) with per-chunk aggregation merged via `functools.reduce`.
- Unknown `user_id` in `users.csv` map to org `__unknown__` for org-level counters.
- Tie-breaking for “top” metrics: `Counter.most_common(1)` (arbitrary among ties).

## Project layout

```
README.md
.env.example
data/                    # shared CSV inputs + analytics_results.json (all languages)
python/
  app.py                 # Flask app
  settings.py              # pydantic-settings from env / .env
  pyproject.toml
  uv.lock
  chat/
    chat_analytics.py      # aggregation + cache
    analytics_schemas.py
    cli.py
    question.md            # the assessment
```
