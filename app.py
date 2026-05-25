from flask import Flask, render_template, request, jsonify, Response, stream_with_context
import threading
import json
import time
import os
from queue import Queue
from data_source import (
    get_chart_data,
    get_yfinance_symbols,
    get_yfinance_us_symbols,
    get_hyperliquid_symbols,
    get_binance_symbols,
    get_available_timeframes,
    start_all_feeds,
    get_market_status,
    get_live_price,
)

app = Flask(__name__)
app.config["TEMPLATES_AUTO_RELOAD"] = True

latest_events = []
latest_events_lock = threading.Lock()


def broadcast_market_event(event):
    with latest_events_lock:
        # avoid appending repeated identical events for same source+symbol
        if latest_events:
            last = latest_events[-1]
            if last.get("source") == event.get("source") and last.get("symbol") == event.get("symbol") and last.get("price") == event.get("price"):
                return
        latest_events.append(event)
        if len(latest_events) > 1000:
            latest_events[:] = latest_events[-1000:]


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/config")
def config():
    return jsonify(
        {
            "sources": [
                {"id": "yfinance_india", "label": "Yahoo Finance (India)"},
                {"id": "yfinance_us", "label": "Yahoo Finance (US Stocks)"},
                {"id": "hyperliquid", "label": "Hyperliquid (Crypto)"},
                {"id": "binance", "label": "Binance (Crypto)"},
            ],
            "symbols": {
                "hyperliquid": get_hyperliquid_symbols(),
                "yfinance_india": get_yfinance_symbols(),
                "yfinance_us": get_yfinance_us_symbols(),
                "binance": get_binance_symbols(),
            },
            "timeframes": get_available_timeframes(),
        }
    )


@app.route("/api/chart_data")
def chart_data():
    source = request.args.get("source", "yfinance")
    symbol = request.args.get("symbol", "RELIANCE.NS")
    timeframe = request.args.get("timeframe", "1h")
    data = get_chart_data(source, symbol, timeframe)
    return jsonify({"source": source, "symbol": symbol, "timeframe": timeframe, "bars": data})


@app.route("/stream/market")
def stream_market():
    @stream_with_context
    def event_stream():
        last_index = 0
        while True:
            time.sleep(0.25)
            events_to_send = []
            with latest_events_lock:
                if last_index < len(latest_events):
                    events_to_send = latest_events[last_index:]
                    last_index = len(latest_events)
            if events_to_send:
                for event in events_to_send:
                    yield f"data: {json.dumps(event)}\n\n"
            else:
                yield ": keep-alive\n\n"
    headers = {
        "Cache-Control": "no-cache, no-transform",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no",
    }
    return Response(event_stream(), content_type="text/event-stream; charset=utf-8", headers=headers)


@app.route("/api/market_status")
def api_market_status():
    source = request.args.get("source", "yfinance")
    symbol = request.args.get("symbol", "RELIANCE.NS")
    try:
        status = get_market_status(source, symbol)
        return jsonify(status)
    except Exception as exc:
        return jsonify({"open": False, "error": str(exc)})


@app.route("/api/live_price")
def api_live_price():
    source = request.args.get("source", "yfinance")
    symbol = request.args.get("symbol", "RELIANCE.NS")
    try:
        p = get_live_price(source, symbol)
        return jsonify(p)
    except Exception as exc:
        return jsonify({"price": None, "error": str(exc)})


@app.route("/api/debug_events")
def api_debug_events():
    with latest_events_lock:
        tail = list(latest_events[-50:])
    return jsonify({"count": len(tail), "events": tail})


def hyperliquid_feed_background():
    for update in start_all_feeds():
        broadcast_market_event(update)


if __name__ == "__main__":
    thread = threading.Thread(target=hyperliquid_feed_background, daemon=True)
    thread.start()
    port = int(os.environ.get("PORT", "5000"))
    app.run(host="127.0.0.1", port=port, threaded=True, use_reloader=False)
