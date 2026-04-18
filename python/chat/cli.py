"""CLI entrypoint: compute chat analytics and write JSON."""

from __future__ import annotations

from .chat_analytics import ANALYTICS_RESULTS_FILE, DATA_DIR, run


def main() -> None:
    """Print headline metrics; aggregation uses cache at ``data/analytics_results.json`` when present."""
    result, from_cache, perf = run()

    print("Chat analytics (top metrics)")
    print(f"  Top sender (user):     {result.top_sender_user.id}  ({result.top_sender_user.count})")
    print(f"  Top receiver (user):   {result.top_receiver_user.id}  ({result.top_receiver_user.count})")
    print(f"  Top sender (org):      {result.top_sender_org.id}  ({result.top_sender_org.count})")
    print(f"  Top receiver (org):    {result.top_receiver_org.id}  ({result.top_receiver_org.count})")
    out_path = DATA_DIR / ANALYTICS_RESULTS_FILE
    if from_cache:
        print(f"\nRead {out_path}")
    else:
        assert perf is not None
        print(
            f"[PERF] load_user_org: {perf['load_user_org_seconds']:.3f}s\n"
            f"[PERF] load_channel_participants: {perf['load_channel_participants_seconds']:.3f}s\n"
            f"[PERF] aggregate_messages: {perf['aggregate_messages_seconds']:.3f}s\n"
            f"[PERF] total run: {perf['total_seconds']:.3f}s"
        )
        print(f"\nWrote {out_path}")


if __name__ == "__main__":
    main()
