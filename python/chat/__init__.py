"""Chat analytics: aggregate messaging CSVs into user/org send & receive metrics.

Run from ``python/`` (after ``uv sync``)::

    uv run python -m chat.cli

Input CSVs live in the repo ``data/`` (see ``chat/question.md``). Output is
``data/analytics_results.json``.
"""
