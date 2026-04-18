"""Flask application entrypoint."""

from flask import Flask

from chat.chat_analytics import run
from settings import settings

app = Flask(__name__)


@app.route("/")
def index():
    return {"status": "ok", "message": "AI Chatbot API"}


@app.get("/api/chat/analytics")
def chat_analytics():
    """Return chat analytics: from ``data/analytics_results.json`` when valid, else compute and persist."""
    result, from_cache, perf = run()
    meta: dict[str, object] = {"source": "cache" if from_cache else "computed"}
    if perf is not None:
        meta["perf"] = perf
    return {
        "analytics": result.model_dump(mode="json"),
        "meta": meta,
    }


def main():
    print(settings.model_dump())

    app.run(
        host=settings.host,
        port=settings.port,
        debug=settings.flask_debug,
    )


if __name__ == "__main__":
    main()
