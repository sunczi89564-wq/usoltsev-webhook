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
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,chainlink&vs_currencies=usd&include_24hr_change=true"
        r = requests.get(url, timeout=10).json()
        g = requests.get("https://api.coingecko.com/api/v3/global", timeout=10).json()
        usdt_d = g["data"]["market_cap_percentage"]["usdt"]
        prices = {
            "BTCUSDT": {
                "price": r["bitcoin"]["usd"],
                "change": r["bitcoin"]["usd_24h_change"]
            },
            "ETHUSDT": {
                "price": r["ethereum"]["usd"],
                "change": r["ethereum"]["usd_24h_change"]
            },
            "LINKUSDT": {
                "price": r["chainlink"]["usd"],
                "change": r["chainlink"]["usd_24h_change"]
            },
            "USDT.D": {
                "price": usdt_d,
                "change": 0
            }
        }
        return prices
    except Exception as e:
        return {}

def get_fear_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()
        value = r["data"][0]["value"]
        label = r["data"][0]["value_classification"]
        return value + " - " + label
    except:
        return "N/A"

def send_telegram(text):
    url = "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage"
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
        btc_price = btc.get("price", 0) or 0
        btc_change = btc.get("change", 0) or 0
        eth_price = eth.get("price", 0) or 0
        eth_change = eth.get("change", 0) or 0
        usdt_price = usdt_d.get("price", 0) or 0
        prompt = (
            "Ты торговый аналитик. Дай краткое мнение (3-4 предложения) по сигналу:\n\n"
            "Сигнал: " + signal + "\n"
            "Тикер: " + ticker + "\n"
            "Цена: " + str(price) + "\n\n"
            "Текущий рынок:\n"
            "- BTC: $" + str(btc_price) + " (" + str(round(btc_change, 2)) + "%)\n"
            "- ETH: $" + str(eth_price) + " (" + str(round(eth_change, 2)) + "%)\n"
            "- USDT Dominance: " + str(round(usdt_price, 2)) + "%\n\n"
            "Оцени: качество сигнала, подтверждает ли макро картина, на что обратить внимание. Будь конкретен и краток."
        )
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text
    except Exception as e:
        return "Аналитика недоступна: " + str(e)

def morning_report():
    prices = get_prices()
    fg = get_fear_greed()
    btc = prices.get("BTCUSDT", {})
    eth = prices.get("ETHUSDT", {})
    link = prices.get("LINKUSDT", {})
    usdt_d = prices.get("USDT.D", {})

    btc_price = btc.get("price", 0) or 0
    btc_change = btc.get("change", 0) or 0
    eth_price = eth.get("price", 0) or 0
    eth_change = eth.get("change", 0) or 0
    link_price = link.get("price", 0) or 0
    link_change = link.get("change", 0) or 0
    usdt_price = usdt_d.get("price", 0) or 0

    line = "------------------------------"
    text = (
        "&#9728; <b>УТРЕННИЙ ОБЗОР</b>\n"
        + datetime.now().strftime("%d.%m.%Y") + "\n"
        + line + "\n"
        + "<b>BTC</b>: $" + "{:,.0f}".format(btc_price) + " (" + "{:+.2f}".format(btc_change) + "%)\n"
        + "<b>ETH</b>: $" + "{:,.2f}".format(eth_price) + " (" + "{:+.2f}".format(eth_change) + "%)\n"
        + "<b>LINK</b>: $" + "{:,.2f}".format(link_price) + " (" + "{:+.2f}".format(link_change) + "%)\n"
        + "<b>USDT.D</b>: " + "{:.2f}".format(usdt_price) + "%\n\n"
        + "<b>Индекс страха/жадности:</b> " + fg + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    send_telegram(text)

@app.route("/test_morning")
def test_morning():
    morning_report()
    return "Утренний обзор отправлен!", 200

@app.route("/webhook", methods=["POST"])
def webhook():
    # Принимаем и JSON и текст
    try:
        if request.content_type and "application/json" in request.content_type:
            data = request.json or {}
        else:
            raw = request.data.decode("utf-8")
            import json
            try:
                data = json.loads(raw)
            except:
                data = {"signal": raw, "ticker": "", "price": "", "time": ""}
    except:
        data = {}

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
    btc_price = btc.get("price", 0) or 0
    btc_change = btc.get("change", 0) or 0
    usdt_price = usdt_d.get("price", 0) or 0

    line = "------------------------------"
    text = (
        emoji + " <b>" + sig_text + " - " + ticker + "</b>\n"
        + "&#128176; Цена: <b>$" + str(price) + "</b>\n"
        + "&#128336; Время: " + str(time) + "\n"
        + line + "\n"
        + "&#128202; <b>Рынок сейчас:</b>\n"
        + "BTC: $" + "{:,.0f}".format(btc_price) + " (" + "{:+.2f}".format(btc_change) + "%)\n"
        + "USDT.D: " + "{:.2f}".format(usdt_price) + "%\n"
        + line + "\n"
        + "&#129504; <b>Мнение Claude:</b>\n"
        + opinion + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    send_telegram(text)
    return "OK", 200

@app.route("/")
def index():
    return "Webhook работает!", 200

scheduler = BackgroundScheduler()
scheduler.add_job(morning_report, "cron", hour=6, minute=0)
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
