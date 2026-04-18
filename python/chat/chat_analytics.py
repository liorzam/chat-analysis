"""Chunked CSV chat analytics: top senders/receivers by user and organization.

Domain rules (see ``chat/question.md``)
----------------------------------------
- **messages.csv** rows are *sends*: one row = one message from ``user_id`` in
  ``channel_id``.
- **Received** counts are *not* stored in the CSV. A message in channel C is
  seen by every *other* participant of C. So if C has N members, the sender
  accrues +1 *sent* and each of the (N - 1) other members accrues +1 *received*.
- **Organizations**: each user maps to ``org_id`` via **users.csv**. Org-level
  sent/received totals are the sums of their users' user-level totals.

Pipeline
--------
1. Load **users** and **channel_participants** once (small tables).
2. Stream **messages** in chunks so we never load the full messages file.
3. Per chunk, aggregate sends/receives, then merge chunk totals with
   ``functools.reduce`` (same result as one global loop, but bounded memory).

Output is a :class:`chat.analytics_schemas.ChatAnalyticsResult` (Pydantic).
:class:`run` reads valid JSON from ``data/analytics_results.json`` when present;
otherwise :func:`compute_analytics` runs and persists there.
"""

from __future__ import annotations

import json
import time
from collections import Counter
from collections.abc import Iterator, Mapping, Sequence
from functools import reduce
from json import JSONDecodeError
from pathlib import Path
from typing import TypedDict

import pandas as pd
from pydantic import ValidationError

from settings import settings

from .analytics_schemas import ChatAnalyticsResult, RankedMetric

# Input CSVs and output JSON (path from ``settings.data_dir``, default ``./data``).
DATA_DIR = settings.data_dir

# Persisted headline metrics next to input CSVs (see :func:`run`).
ANALYTICS_RESULTS_FILE = "analytics_results.json"

# Rows per pandas chunk when reading messages; trades RAM vs. number of iterations.
MESSAGES_CHUNK_SIZE = 25_000

# Fallback org when ``user_id`` is missing from users.csv (bad data).
UNKNOWN_ORG = "__unknown__"

# Order: sent-by-user, received-by-user, sent-by-org, received-by-org.
Counters = tuple[Counter[str], Counter[str], Counter[str], Counter[str]]


class AnalyticsComputePerf(TypedDict):
    """Wall-clock seconds for each phase of :func:`compute_analytics`."""

    load_user_org_seconds: float
    load_channel_participants_seconds: float
    aggregate_messages_seconds: float
    total_seconds: float


def load_user_org(data_dir: Path) -> dict[str, str]:
    """Map each chat user to their organization (for org-level metrics)."""
    path = data_dir / "users.csv"
    df = pd.read_csv(path, usecols=["user_id", "org_id"])
    return dict(zip(df["user_id"].astype(str), df["org_id"].astype(str), strict=True))


def load_channel_participants(data_dir: Path) -> dict[str, list[str]]:
    """Map each channel to the list of users who participate (receive broadcasts there).

    Used to know, for a message in channel C, who counts as *receiver* (everyone
    in the list except the sender).
    """
    path = data_dir / "channel_participants.csv"
    df = pd.read_csv(path, dtype=str)
    return {
        str(ch): grp.astype(str).tolist()
        for ch, grp in df.groupby("channel_id", sort=False)["user_id"]
    }


def empty_counters() -> Counters:
    """Fresh zero counters for one chunk's contribution."""
    return Counter(), Counter(), Counter(), Counter()


def merge_counters(left: Counters, right: Counters) -> Counters:
    """Combine two partial aggregates (e.g. chunk A + chunk B).

    Uses ``Counter`` addition so we do not mutate previous chunk totals in place.
    """
    return (
        left[0] + right[0],
        left[1] + right[1],
        left[2] + right[2],
        left[3] + right[3],
    )


def fold_chunk(
    chunk: pd.DataFrame,
    channel_users: Mapping[str, Sequence[str]],
    user_org: Mapping[str, str],
) -> Counters:
    """Turn one messages chunk into four counter deltas (user/org × sent/received).

    **Send side:** For each distinct ``(channel_id, user_id)`` in this chunk we
    count *k* identical sends (same sender, same channel). The sender gets *k*
    sent (user + org).

    **Receive side:** For each such group, every participant of that channel
    except the sender gets *k* received (user + org). That matches *k* separate
    messages, each delivering one receive to each other participant.

    Grouping by ``(channel_id, user_id)`` is an optimization: we apply *k* in one
    step instead of repeating the participant loop *k* times for duplicate rows.
    """
    sent_user, recv_user, sent_org, recv_org = empty_counters()
    org_get = user_org.get

    # k = number of message rows in this chunk for (channel, sender).
    grouped = chunk.groupby(["channel_id", "user_id"], sort=False).size()

    for (ch, sender), k in grouped.items():
        k = int(k)
        sender = str(sender)
        ch = str(ch)

        # Sent metrics: only the author of the message.
        sent_user[sender] += k
        sent_org[org_get(sender, UNKNOWN_ORG)] += k

        participants = channel_users.get(ch)
        if not participants:
            # No roster for this channel: still counted sends above; no receivers.
            continue

        # Received metrics: every other channel member gets this message (k times).
        for u in participants:
            if u != sender:
                recv_user[u] += k
                recv_org[org_get(u, UNKNOWN_ORG)] += k

    return sent_user, recv_user, sent_org, recv_org


def message_chunks(data_dir: Path) -> Iterator[pd.DataFrame]:
    """Yield successive slices of messages.csv (single pass over the file on disk).

    Only ``channel_id`` and ``user_id`` are parsed (columns needed for stats).
    """
    path = data_dir / "messages.csv"
    return pd.read_csv(
        path,
        chunksize=MESSAGES_CHUNK_SIZE,
        usecols=["channel_id", "user_id"],
        dtype=str,
    )


def aggregate_messages(
    data_dir: Path,
    user_org: Mapping[str, str],
    channel_users: Mapping[str, Sequence[str]],
) -> Counters:
    """Fold all message chunks into final user/org sent and received counters."""
    return reduce(
        merge_counters,
        (fold_chunk(chunk, channel_users, user_org) for chunk in message_chunks(data_dir)),
        empty_counters(),
    )


def top_one(counter: Counter[str]) -> RankedMetric:
    """Pick the id with highest count; ties break arbitrarily (``most_common(1)``)."""
    if not counter:
        return RankedMetric(id="", count=0)
    entity_id, count = counter.most_common(1)[0]
    return RankedMetric(id=entity_id, count=int(count))


def _analytics_results_path(data_dir: Path) -> Path:
    return data_dir / ANALYTICS_RESULTS_FILE


def _try_load_cached(data_dir: Path) -> ChatAnalyticsResult | None:
    path = _analytics_results_path(data_dir)
    if not path.is_file():
        return None
    try:
        return ChatAnalyticsResult.model_validate_json(path.read_text(encoding="utf-8"))
    except (OSError, JSONDecodeError, ValidationError):
        return None


def compute_analytics(data_dir: Path | None = None) -> tuple[ChatAnalyticsResult, AnalyticsComputePerf]:
    """Load reference tables, stream messages, return headline metrics and phase timings."""
    start_time = time.perf_counter()

    base = data_dir or DATA_DIR

    load_user_org_start = time.perf_counter()
    user_org = load_user_org(base)
    load_user_org_end = time.perf_counter()

    load_channel_users_start = time.perf_counter()
    channel_users = load_channel_participants(base)
    load_channel_users_end = time.perf_counter()

    aggregate_msgs_start = time.perf_counter()
    sent_u, recv_u, sent_o, recv_o = aggregate_messages(base, user_org, channel_users)
    aggregate_msgs_end = time.perf_counter()

    end_time = time.perf_counter()

    perf: AnalyticsComputePerf = {
        "load_user_org_seconds": load_user_org_end - load_user_org_start,
        "load_channel_participants_seconds": load_channel_users_end - load_channel_users_start,
        "aggregate_messages_seconds": aggregate_msgs_end - aggregate_msgs_start,
        "total_seconds": end_time - start_time,
    }

    result = ChatAnalyticsResult(
        top_sender_user=top_one(sent_u),
        top_receiver_user=top_one(recv_u),
        top_sender_org=top_one(sent_o),
        top_receiver_org=top_one(recv_o),
    )
    return result, perf


def run(
    data_dir: Path | None = None,
) -> tuple[ChatAnalyticsResult, bool, AnalyticsComputePerf | None]:
    """Return headline metrics, using ``analytics_results.json`` when valid; else compute and persist.

    Second value is ``True`` when loaded from disk. Third is phase timings when freshly computed,
    otherwise ``None``.
    """
    base = data_dir or DATA_DIR
    cached = _try_load_cached(base)
    if cached is not None:
        return cached, True, None

    result, perf = compute_analytics(base)
    out_path = _analytics_results_path(base)
    out_path.write_text(
        json.dumps(result.model_dump(mode="json"), indent=2),
        encoding="utf-8",
    )
    return result, False, perf
