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
NEWS_API_KEY      = os.environ.get("NEWS_API_KEY")

def get_crypto_prices():
    try:
        url = "https://min-api.cryptocompare.com/data/pricemultifull?fsyms=BTC&tsyms=USD"
        r = requests.get(url, timeout=10).json()
        raw = r["RAW"]
        return {
            "price": raw["BTC"]["USD"]["PRICE"],
            "change": raw["BTC"]["USD"]["CHANGEPCT24HOUR"]
        }
    except:
        return {"price": 0, "change": 0}

def get_btc_dominance():
    try:
        r = requests.get("https://api.coingecko.com/api/v3/global", timeout=10).json()
        return round(r["data"]["market_cap_percentage"]["btc"], 2)
    except:
        try:
            url = "https://min-api.cryptocompare.com/data/top/totalvolfull?limit=20&tsym=USD"
            r = requests.get(url, timeout=10).json()
            total = sum([c["RAW"]["USD"]["MKTCAP"] for c in r["Data"] if "RAW" in c and "USD" in c["RAW"]])
            btc = next((c["RAW"]["USD"]["MKTCAP"] for c in r["Data"] if c["CoinInfo"]["Name"] == "BTC" and "RAW" in c), 0)
            return round(btc / total * 100, 2) if total > 0 else 0
        except:
            return 0

def get_traditional_prices():
    results = {}
    pairs = {
        "GC=F":      "gold",
        "BZ=F":      "brent",
        "DX-Y.NYB":  "dxy",
        "USDRUB=X":  "usdrub",
        "AEDUSД=X":  "aedusd"
    }
    for symbol, key in pairs.items():
        try:
            url = "https://query1.finance.yahoo.com/v8/finance/chart/" + symbol + "?interval=1d&range=5d"
            headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"}
            r = requests.get(url, headers=headers, timeout=8).json()
            closes = r["chart"]["result"][0]["indicators"]["quote"][0]["close"]
            closes = [x for x in closes if x is not None]
            if len(closes) >= 2:
                price = closes[-1]
                prev = closes[-2]
                change = (price - prev) / prev * 100
            elif len(closes) == 1:
                price = closes[-1]
                change = 0
            else:
                price = 0
                change = 0
            results[key] = {"price": round(price, 4), "change": round(change, 2)}
        except:
            results[key] = {"price": 0, "change": 0}
    return results

def get_fear_greed():
    try:
        r = requests.get("https://api.alternative.me/fng/?limit=1", timeout=5).json()
        value = r["data"][0]["value"]
        label = r["data"][0]["value_classification"]
        return value + " - " + label
    except:
        return "N/A"

def get_news():
    try:
        url = (
            "https://newsapi.org/v2/everything"
            "?q=bitcoin+OR+crypto+OR+fed+OR+gold+OR+oil"
            "&language=en"
            "&sortBy=publishedAt"
            "&pageSize=5"
            "&apiKey=" + NEWS_API_KEY
        )
        r = requests.get(url, timeout=8).json()
        articles = r.get("articles", [])
        news_lines = []
        for a in articles[:3]:
            title = a.get("title", "")
            if title and "[Removed]" not in title:
                # Обрезаем длинные заголовки
                if len(title) > 80:
                    title = title[:80] + "..."
                news_lines.append("- " + title)
        return "\n".join(news_lines) if news_lines else "Новости недоступны"
    except:
        return "Новости недоступны"

def send_telegram(text):
    url = "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage"
    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    })

def get_claude_opinion(signal, ticker, price, btc_price, btc_change, btc_d):
    try:
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
        prompt = (
            "Ты торговый аналитик. Дай краткое мнение (3-4 предложения) по сигналу:\n\n"
            "Сигнал: " + signal + "\n"
            "Тикер: " + ticker + "\n"
            "Цена: " + str(price) + "\n\n"
            "Текущий рынок:\n"
            "- BTC: $" + str(btc_price) + " (" + str(round(btc_change, 2)) + "%)\n"
            "- BTC Dominance: " + str(btc_d) + "%\n\n"
            "Оцени: качество сигнала, подтверждает ли макро картина, на что обратить внимание. Будь конкретен и краток."
        )
        message = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=300,
            messages=[{"role": "user", "content": prompt}]
        )
        return message.content[0].text
    except Exception as e:
        return "Аналитика временно недоступна"

def daily_report():
    btc = get_crypto_prices()
    btc_d = get_btc_dominance()
    trad = get_traditional_prices()
    fg = get_fear_greed()
    news = get_news()

    btc_price  = btc.get("price", 0) or 0
    btc_change = btc.get("change", 0) or 0

    gold   = trad.get("gold",   {})
    brent  = trad.get("brent",  {})
    dxy    = trad.get("dxy",    {})
    usdrub = trad.get("usdrub", {})
    aedusd = trad.get("aedusd", {})

    gold_price   = gold.get("price", 0) or 0
    gold_change  = gold.get("change", 0) or 0
    brent_price  = brent.get("price", 0) or 0
    brent_change = brent.get("change", 0) or 0
    dxy_price    = dxy.get("price", 0) or 0
    dxy_change   = dxy.get("change", 0) or 0
    rub_price    = usdrub.get("price", 0) or 0
    rub_change   = usdrub.get("change", 0) or 0
    aed_price    = aedusd.get("price", 3.6725) or 3.6725
    aed_change   = aedusd.get("change", 0) or 0
    rub_aed      = round(rub_price / aed_price, 2) if aed_price > 0 else 0

    now = datetime.now()
    hour = now.hour
    if hour == 4:
        greeting = "&#9728; <b>УТРЕННИЙ ДАЙДЖЕСТ</b>"
    elif hour == 10:
        greeting = "&#127774; <b>ДНЕВНОЙ ДАЙДЖЕСТ</b>"
    else:
        greeting = "&#127762; <b>ВЕЧЕРНИЙ ДАЙДЖЕСТ</b>"

    line = "------------------------------"
    text = (
        greeting + "\n"
        + now.strftime("%d.%m.%Y  %H:%M") + " (UTC+5)\n"
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
        + "&#128240; <b>НОВОСТИ</b>\n"
        + news + "\n"
        + line + "\n"
        + "&#128561; <b>Индекс страха/жадности:</b> " + fg + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    send_telegram(text)

@app.route("/test_morning")
def test_morning():
    daily_report()
    return "Дайджест отправлен!", 200

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

# Дайджест 3 раза в день по Екатеринбургу (UTC+5)
# 09:00 = 04:00 UTC
# 15:00 = 10:00 UTC
# 19:00 = 14:00 UTC
scheduler = BackgroundScheduler()
scheduler.add_job(daily_report, "cron", hour=4,  minute=0)
scheduler.add_job(daily_report, "cron", hour=10, minute=0)
scheduler.add_job(daily_report, "cron", hour=14, minute=0)
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
