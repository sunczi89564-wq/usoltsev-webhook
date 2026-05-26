from flask import Flask, request
import requests
import os
import anthropic
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import json

app = Flask(__name__)

BOT_TOKEN         = os.environ.get("BOT_TOKEN")
CHAT_ID           = os.environ.get("CHAT_ID")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

def get_crypto_prices():
    try:
        url = "https://min-api.cryptocompare.com/data/pricemultifull?fsyms=BTC&tsyms=USD"
        r = requests.get(url, timeout=10).json()
        raw = r["RAW"]
        btc_price = raw["BTC"]["USD"]["PRICE"]
        btc_change = raw["BTC"]["USD"]["CHANGEPCT24HOUR"]
        return {"price": btc_price, "change": btc_change}
    except:
        return {"price": 0, "change": 0}

def get_btc_dominance():
    try:
        r = requests.get("https://min-api.cryptocompare.com/data/top/mktcapfull?limit=10&tsym=USD", timeout=10).json()
        total = 0
        btc_cap = 0
        for coin in r["Data"]:
            cap = coin["RAW"]["USD"]["MKTCAP"]
            total += cap
            if coin["CoinInfo"]["Name"] == "BTC":
                btc_cap = cap
        dominance = (btc_cap / total * 100) if total > 0 else 0
        return round(dominance, 2)
    except:
        return 0

def get_traditional_prices():
    try:
        # Золото и нефть через Yahoo Finance
        results = {}
        pairs = {
            "GC=F": "gold",
            "BZ=F": "brent",
            "DX-Y.NYB": "dxy",
            "USDRUB=X": "usdrub",
            "AEDUSД=X": "aedusd"
        }
        for symbol, key in pairs.items():
            try:
                url = "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol + "?interval=1d&range=2d"
                headers = {"User-Agent": "Mozilla/5.0"}
                r = requests.get(url, headers=headers, timeout=8).json()
                closes = r["chart"]["result"][0]["indicators"]["quote"][0]["close"]
                closes = [x for x in closes if x is not None]
                if len(closes) >= 2:
                    price = closes[-1]
                    prev = closes[-2]
                    change = (price - prev) / prev * 100
                else:
                    price = closes[-1] if closes else 0
                    change = 0
                results[key] = {"price": price, "change": change}
            except:
                results[key] = {"price": 0, "change": 0}
        return results
    except:
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

def get_claude_opinion(signal, ticker, price, btc_price, btc_change, usdt_d):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt = (
            "Ты торговый аналитик. Дай краткое мнение (3-4 предложения) по сигналу:\n\n"
            "Сигнал: " + signal + "\n"
            "Тикер: " + ticker + "\n"
            "Цена: " + str(price) + "\n\n"
            "Текущий рынок:\n"
            "- BTC: $" + str(btc_price) + " (" + str(round(btc_change, 2)) + "%)\n"
            "- BTC Dominance: " + str(usdt_d) + "%\n\n"
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
    btc = get_crypto_prices()
    btc_d = get_btc_dominance()
    trad = get_traditional_prices()
    fg = get_fear_greed()

    btc_price  = btc.get("price", 0) or 0
    btc_change = btc.get("change", 0) or 0

    gold       = trad.get("gold", {})
    brent      = trad.get("brent", {})
    dxy        = trad.get("dxy", {})
    usdrub     = trad.get("usdrub", {})
    aedusd     = trad.get("aedusd", {})

    gold_price   = gold.get("price", 0) or 0
    gold_change  = gold.get("change", 0) or 0
    brent_price  = brent.get("price", 0) or 0
    brent_change = brent.get("change", 0) or 0
    dxy_price    = dxy.get("price", 0) or 0
    dxy_change   = dxy.get("change", 0) or 0

    # RUB/USD и AED/USD
    rub_price  = usdrub.get("price", 0) or 0
    rub_change = usdrub.get("change", 0) or 0
    aed_price  = aedusd.get("price", 3.6725) or 3.6725
    aed_change = aedusd.get("change", 0) or 0

    # RUB/AED = RUB/USD / AED/USD * обратно
    # 1 USD = rub_price RUB, 1 USD = aed_price AED
    # 1 AED = rub_price / aed_price RUB
    rub_aed = (rub_price / aed_price) if aed_price > 0 else 0

    line = "------------------------------"
    text = (
        "&#9728; <b>УТРЕННИЙ ОБЗОР</b>\n"
        + datetime.now().strftime("%d.%m.%Y") + "\n"
        + line + "\n"
        + "&#129377; <b>КРИПТО</b>\n"
        + "BTC: $" + "{:,.0f}".format(btc_price) + " (" + "{:+.2f}".format(btc_change) + "%)\n"
        + "BTC.D: " + "{:.2f}".format(btc_d) + "%\n"
        + line + "\n"
        + "&#127758; <b>МАКРО</b>\n"
        + "DXY: " + "{:.2f}".format(dxy_price) + " (" + "{:+.2f}".format(dxy_change) + "%)\n"
        + "&#129351; Золото: $" + "{:,.2f}".format(gold_price) + " (" + "{:+.2f}".format(gold_change) + "%)\n"
        + "&#128738; Нефть Brent: $" + "{:.2f}".format(brent_price) + " (" + "{:+.2f}".format(brent_change) + "%)\n"
        + line + "\n"
        + "&#128178; <b>ВАЛЮТЫ</b>\n"
        + "USD/RUB: " + "{:.2f}".format(rub_price) + " (" + "{:+.2f}".format(rub_change) + "%)\n"
        + "USD/AED: " + "{:.4f}".format(aed_price) + " (" + "{:+.2f}".format(aed_change) + "%)\n"
        + "AED/RUB: " + "{:.2f}".format(rub_aed) + "\n"
        + line + "\n"
        + "&#128561; <b>Индекс страха/жадности:</b> " + fg + "\n"
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
    try:
        if request.content_type and "application/json" in request.content_type:
            data = request.json or {}
        else:
            raw = request.data.decode("utf-8")
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

    btc = get_crypto_prices()
    btc_d = get_btc_dominance()
    btc_price = btc.get("price", 0) or 0
    btc_change = btc.get("change", 0) or 0

    opinion = get_claude_opinion(signal, ticker, price, btc_price, btc_change, btc_d)

    line = "------------------------------"
    text = (
        emoji + " <b>" + sig_text + " - " + ticker + "</b>\n"
        + "&#128176; Цена: <b>$" + str(price) + "</b>\n"
        + "&#128336; Время: " + str(time) + "\n"
        + line + "\n"
        + "&#128202; <b>Рынок сейчас:</b>\n"
        + "BTC: $" + "{:,.0f}".format(btc_price) + " (" + "{:+.2f}".format(btc_change) + "%)\n"
        + "BTC.D: " + "{:.2f}".format(btc_d) + "%\n"
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
