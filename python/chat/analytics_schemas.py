"""Pydantic models for persisted chat analytics output.

These mirror the four headline answers from the problem statement:
top sender/receiver as *users*, and top sender/receiver as *organizations*.
``model_dump(mode="json")`` produces JSON-serializable dicts (including UTC
``generated_at``).
"""

from datetime import datetime, timezone

from pydantic import BaseModel, Field


class RankedMetric(BaseModel):
    """One leaderboard entry: who (``id``) and how many messages (``count``)."""

    id: str
    count: int


class ChatAnalyticsResult(BaseModel):
    """Full result object written to ``data/analytics_results.json``."""

    generated_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="UTC time when the aggregation finished.",
    )
    top_sender_user: RankedMetric = Field(
        description="User id with the largest number of *sent* messages.",
    )
    top_receiver_user: RankedMetric = Field(
        description="User id with the largest number of *received* messages.",
    )
    top_sender_org: RankedMetric = Field(
        description="Org id whose members sent the most messages in total.",
    )
    top_receiver_org: RankedMetric = Field(
        description="Org id whose members received the most messages in total.",
    )
