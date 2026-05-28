from flask import Flask, request
import requests
import os
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import json
import xml.etree.ElementTree as ET
import threading

app = Flask(__name__)

BOT_TOKEN    = os.environ.get("BOT_TOKEN")
CHAT_ID      = os.environ.get("CHAT_ID")
GROQ_API_KEY = os.environ.get("GROQ_API_KEY")

# Буфер сигналов
signal_buffer = []
buffer_lock = threading.Lock()
buffer_timer = None
BUFFER_SECONDS = 60

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
        return 0

def get_altseason_index():
    try:
        url = "https://blockchaincenter.net/altcoin-season-index/"
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=8)
        # Ищем значение индекса в HTML
        text = r.text
        idx = text.find('"altcoinSeason":')
        if idx != -1:
            val = text[idx+16:idx+19].strip().rstrip(',').rstrip('}')
            return int(val)
        # Второй вариант парсинга
        idx = text.find("altcoin-season-index-value")
        if idx != -1:
            snippet = text[idx:idx+100]
            for part in snippet.split(">"):
                clean = part.replace("</span", "").replace("</div", "").strip()
                if clean.isdigit():
                    return int(clean)
        return None
    except:
        return None

def get_total3():
    try:
        # Total3 = общая капитализация минус BTC и ETH
        r = requests.get("https://api.coingecko.com/api/v3/global", timeout=10).json()
        total = r["data"]["total_market_cap"]["usd"]
        btc_pct = r["data"]["market_cap_percentage"]["btc"] / 100
        eth_pct = r["data"]["market_cap_percentage"]["eth"] / 100
        total3 = total * (1 - btc_pct - eth_pct)
        # Изменение за 24ч
        total_change = r["data"]["market_cap_change_percentage_24h_usd"]
        return {"value": total3, "change": total_change}
    except:
        return {"value": 0, "change": 0}

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
    sources = [
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://cointelegraph.com/rss",
        "https://decrypt.co/feed",
    ]
    headlines = []
    for url in sources:
        if len(headlines) >= 3:
            break
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers, timeout=6)
            root = ET.fromstring(r.content)
            items = root.findall(".//item")
            for item in items[:2]:
                title = item.find("title")
                if title is not None and title.text:
                    text = title.text.strip()
                    if len(text) > 80:
                        text = text[:80] + "..."
                    headlines.append("- " + text)
                    if len(headlines) >= 3:
                        break
        except:
            continue
    return "\n".join(headlines) if headlines else "Новости временно недоступны"

def send_telegram(text):
    url = "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage"
    requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    })

def get_groq_opinion(signals_text, btc_price, btc_change, btc_d):
    try:
        headers = {
            "Authorization": "Bearer " + GROQ_API_KEY,
            "Content-Type": "application/json"
        }
        prompt = (
            "Ты торговый аналитик. Дай краткое мнение (4-5 предложений) на русском языке по следующим сигналам:\n\n"
            + signals_text + "\n\n"
            "Текущий рынок:\n"
            "- BTC: $" + str(btc_price) + " (" + str(round(btc_change, 2)) + "%)\n"
            "- BTC Dominance: " + str(btc_d) + "%\n\n"
            "Оцени: общую картину по всем сигналам, есть ли подтверждение между монетами, "
            "что говорит макро, на что обратить внимание. Будь конкретен и краток."
        )
        data = {
            "model": "llama3-8b-8192",
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": 400
        }
        r = requests.post("https://api.groq.com/openai/v1/chat/completions",
                         headers=headers, json=data, timeout=15)
        result = r.json()
        return result["choices"][0]["message"]["content"]
    except:
        return "Аналитика временно недоступна"

def process_buffer():
    global signal_buffer, buffer_timer

    with buffer_lock:
        if not signal_buffer:
            return
        signals = signal_buffer.copy()
        signal_buffer = []
        buffer_timer = None

    btc = get_crypto_prices()
    btc_d = get_btc_dominance()
    btc_price = btc.get("price", 0) or 0
    btc_change = btc.get("change", 0) or 0

    line = "------------------------------"
    signals_header = ""
    signals_text_for_ai = ""

    for s in signals:
        signal = s.get("signal", "")
        ticker = s.get("ticker", "")
        price  = s.get("price", "")

        if "LONG_STRONG" in signal:
            emoji = "&#128994;"
            sig_text = "ЛОНГ"
        else:
            emoji = "&#128308;"
            sig_text = "ШОРТ"

        signals_header += emoji + " <b>" + sig_text + " - " + ticker + "</b>  $" + str(price) + "\n"
        signals_text_for_ai += sig_text + " - " + ticker + " $" + str(price) + "\n"

    opinion = get_groq_opinion(signals_text_for_ai, btc_price, btc_change, btc_d)

    count = str(len(signals))
    text = (
        "&#9889; <b>ПАКЕТ СИГНАЛОВ (" + count + ")</b>\n"
        + line + "\n"
        + signals_header
        + line + "\n"
        + "&#128202; <b>Рынок сейчас:</b>\n"
        + "BTC: $" + "{:,.0f}".format(btc_price) + " (" + "{:+.2f}".format(btc_change) + "%)\n"
        + "BTC.D: " + "{:.2f}".format(btc_d) + "%\n"
        + line + "\n"
        + "&#129504; <b>Мнение ИИ:</b>\n"
        + opinion + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    send_telegram(text)

def format_total3(value):
    if value >= 1_000_000_000_000:
        return "{:.2f}T".format(value / 1_000_000_000_000)
    elif value >= 1_000_000_000:
        return "{:.2f}B".format(value / 1_000_000_000)
    else:
        return "{:.2f}M".format(value / 1_000_000)

def format_altseason(value):
    if value is None:
        return "N/A"
    if value >= 75:
        return str(value) + " - Альтсезон &#127881;"
    elif value >= 50:
        return str(value) + " - Нейтрально"
    elif value >= 25:
        return str(value) + " - Ближе к BTC"
    else:
        return str(value) + " - Сезон BTC &#128834;"

def daily_report():
    btc = get_crypto_prices()
    btc_d = get_btc_dominance()
    trad = get_traditional_prices()
    fg = get_fear_greed()
    news = get_news()
    altseason = get_altseason_index()
    total3 = get_total3()

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

    total3_val    = total3.get("value", 0) or 0
    total3_change = total3.get("change", 0) or 0

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
        + "Total3: " + format_total3(total3_val) + " (" + "{:+.2f}".format(total3_change) + "%)\n"
        + "Альтсезон: " + format_altseason(altseason) + "\n"
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
    global signal_buffer, buffer_timer

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

    with buffer_lock:
        signal_buffer.append(data)
        if buffer_timer is None:
            t = threading.Timer(BUFFER_SECONDS, process_buffer)
            t.daemon = True
            t.start()
            buffer_timer = t

    return "OK", 200

@app.route("/")
def index():
    return "Webhook работает!", 200

scheduler = BackgroundScheduler()
scheduler.add_job(daily_report, "cron", hour=4,  minute=0)
scheduler.add_job(daily_report, "cron", hour=10, minute=0)
scheduler.add_job(daily_report, "cron", hour=14, minute=0)
scheduler.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
