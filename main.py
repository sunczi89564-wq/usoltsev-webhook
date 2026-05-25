from flask import Flask, request
import requests
import os
import anthropic
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime

app = Flask(__name__)

BOT_TOKEN         = os.environ.get("BOT_TOKEN")
CHAT_ID           = os.environ.get("CHAT_ID")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

def get_prices():
    try:
        symbols = ["BTCUSDT", "ETHUSDT", "LINKUSDT"]
        prices = {}
        for symbol in symbols:
            url = f"https://api.binance.com/api/v3/ticker/24hr?symbol={symbol}"
            r = requests.get(url, timeout=5).json()
            prices[symbol] = {
                "price": float(r["lastPrice"]),
                "change": float(r["priceChangePercent"])
            }
        r = requests.get("https://api.coingecko.com/api/v3/global", timeout=5).json()
        usdt_d = r["data"]["market_cap_percentage"]["usdt"]
        prices["USDT.D"] = {"price": usdt_d, "change": 0}
        return prices
    except:
        return {}

def get_fear_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()
        value = r["data"][0]["value"]
        label = r["data"][0]["value_classification"]
        return f"{value} — {label}"
    except:
        return "N/A"

def send_telegram(text):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    })

def get_claude_opinion(signal, ticker, price, prices):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        btc = prices.get("BTCUSDT", {})
        eth = prices.get("ETHUSDT", {})
        usdt_d = prices.get("USDT.D", {})
        prompt = f"""Ты торговый аналитик. Дай краткое мнение (3-4 предложения) по сигналу:

Сигнал: {signal}
Тикер: {ticker}
Цена: {price}

Текущий рынок:
- BTC: ${btc.get('price', 'N/A')} ({btc.get('change', 0):+.2f}%)
- ETH: ${eth.get('price', 'N/A')} ({eth.get('change', 0):+.2f}%)
- USDT Dominance: {usdt_d.get('price', 'N/A'):.2f}%

Оцени: качество сигнала, подтверждает ли макро картина, на что обратить внимание. Будь конкретен и краток."""
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text
    except Exception as e:
        return f"Аналитика недоступна: {str(e)}"

def morning_report():
    prices = get_prices()
    fg = get_fear_greed()
    btc = prices.get("BTCUSDT", {})
    eth = prices.get("ETHUSDT", {})
    link = prices.get("LINKUSDT", {})
    usdt_d = prices.get("USDT.D", {})

    def arrow(change):
        return "+" if change >= 0 else ""

    text = f"""&#9728; <b>УТРЕННИЙ ОБЗОР</b>
{datetime.now().strftime('%d.%m.%Y')}
——————————————
<b>BTC</b>: ${btc.get('price', 'N/A'):,.0f} ({arrow(btc.get('change',0))}{btc.get('change', 0):.2f}%)
<b>ETH</b>: ${eth.get('price', 'N/A'):,.2f} ({arrow(eth.get('change',0))}{eth.get('change', 0):.2f}%)
<b>LINK</b>: ${link.get('price', 'N/A'):,.2f} ({arrow(link.get('change',0))}{link.get('change', 0):.2f}%)
<b>USDT.D</b>: {usdt_d.get('price', 0):.2f}%

<b>Индекс страха/жадности:</b> {fg}
——————————————
<i>Usoltsev Signals</i>"""
    send_telegram(text)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    signal = data.get("signal", "")
    ticker = data.get("ticker", "")
    price  = data.get("price", "")
    time   = data.get("time", "")

    if "LONG_STRONG" in signal:
        emoji = "&#128994;"
        sig_text = "ЛОНГ СИЛЬНЫЙ"
    elif "SHORT_STRONG" in signal:
        emoji = "&#128308;"
        sig_text = "ШОРТ СИЛЬНЫЙ"
    elif "LONG_WEAK" in signal:
        emoji = "&#128993;"
        sig_text = "ЛОНГ СЛАБЫЙ"
    else:
        emoji = "&#128992;"
        sig_text = "ШОРТ СЛАБЫЙ"

    prices = get_prices()
    opinion = get_claude_opinion(signal, ticker, price, prices)
    btc = prices.get("BTCUSDT", {})
    usdt_d = prices.get("USDT.D", {})

    def arrow(change):
        return "+" if change >= 0 else ""

    text = f"""{emoji} <b>{sig_text} — {ticker}</b>
&#128176; Цена: <b>${price}</b>
&#128336; Время: {time}
——————————————
&#128202; <b>Рынок сейчас:</b>
BTC: ${btc.get('price', 0):,.0f} ({arrow(btc.get('change',0))}{btc.get('change', 0):.2f}%)
USDT.D: {usdt_d.get('price', 0):.2f}%
——————————————
&#129504; <b>Мнение Claude:</b>
{opinion}
——————————————
<i>Usoltsev Signals</i>"""

    send_telegram(text)
    return "OK", 200
    
@app.route("/test_morning")
def test_morning():
    morning_report()
    return "Утренний обзор отправлен!", 200

@app.route("/")
def index():
    return "Webhook работает!", 200

scheduler = BackgroundScheduler()
scheduler.add_job(morning_report, 'cron', hour=6, minute=0)
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
