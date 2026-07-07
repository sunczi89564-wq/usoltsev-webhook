from flask import Flask, request
import requests
import os
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime
import json
import xml.etree.ElementTree as ET
import threading
import time

app = Flask(__name__)

BOT_TOKEN         = os.environ.get("BOT_TOKEN")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY")

# ═══════════════════════════════════════════════
# CHAT ID и THREAD ID
# ═══════════════════════════════════════════════
GROUP_CHAT_ID   = "-1004230629656"   # Usoltsev Finance
THREAD_GENERAL  = None               # General — дайджест
THREAD_SCALP    = 2                  # Scalp — 15м и ниже
THREAD_INTRADAY = 4                  # Intraday — 30м-4ч
THREAD_HODL     = 8                  # HODL — 12ч+
THREAD_RU       = 15                 # RU Market
THREAD_US       = 18                 # US Market

# Таймфреймы → топики (для крипто)
TF_TO_THREAD = {
    "1":   THREAD_SCALP,
    "3":   THREAD_SCALP,
    "5":   THREAD_SCALP,
    "15":  THREAD_SCALP,
    "30":  THREAD_INTRADAY,
    "60":  THREAD_INTRADAY,
    "120": THREAD_INTRADAY,
    "240": THREAD_INTRADAY,
    "720": THREAD_HODL,
    "D":   THREAD_HODL,
    "W":   THREAD_HODL,
}

signal_buffer = []
buffer_lock = threading.Lock()
buffer_timer = None
BUFFER_SECONDS = 60

# ═══════════════════════════════════════════════
# ТИКЕРЫ
# ═══════════════════════════════════════════════
RUSSIAN_TICKERS = ["LKOH", "SBER", "GAZP", "ROSN", "NVTK", "GMKN", "YNDX",
                   "TATN", "MAGN", "CHMF", "ALRS", "MTSS", "POLY", "PLZL"]

US_TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOGL", "META",
              "NFLX", "AMD", "INTC", "SPY", "QQQ", "BABA"]

def get_ticker_thread(ticker, tf="60"):
    t = ticker.upper()
    if any(t.startswith(r) for r in RUSSIAN_TICKERS):
        return THREAD_RU
    if any(t.startswith(u) for u in US_TICKERS):
        return THREAD_US
    return TF_TO_THREAD.get(str(tf), THREAD_INTRADAY)

# ═══════════════════════════════════════════════
# КРИПТО ДАННЫЕ
# ═══════════════════════════════════════════════
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
        time.sleep(2)
        return round(r["data"]["market_cap_percentage"]["btc"], 2)
    except:
        return 0

def get_altseason_index():
    try:
        time.sleep(3)
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=50&page=1&price_change_percentage=7d"
        r = requests.get(url, timeout=15).json()
        btc_change = None
        for coin in r:
            if coin["id"] == "bitcoin":
                btc_change = coin.get("price_change_percentage_7d_in_currency", 0) or 0
                break
        if btc_change is None:
            return None
        count = 0
        total = 0
        for coin in r:
            if coin["id"] == "bitcoin":
                continue
            change = coin.get("price_change_percentage_7d_in_currency", None)
            if change is not None:
                total += 1
                if change > btc_change:
                    count += 1
        if total == 0:
            return None
        return round(count / total * 100)
    except:
        return None

def get_total3():
    try:
        time.sleep(2)
        r = requests.get("https://api.coingecko.com/api/v3/global", timeout=10).json()
        total = r["data"]["total_market_cap"]["usd"]
        btc_pct = r["data"]["market_cap_percentage"]["btc"] / 100
        eth_pct = r["data"]["market_cap_percentage"]["eth"] / 100
        total3 = total * (1 - btc_pct - eth_pct)
        total_change = r["data"]["market_cap_change_percentage_24h_usd"]
        return {"value": total3, "change": total_change}
    except:
        return {"value": 0, "change": 0}

# ═══════════════════════════════════════════════
# МАКРО ДАННЫЕ
# ═══════════════════════════════════════════════
def get_traditional_prices():
    results = {}
    pairs = {
        "GC=F":      "gold",
        "BZ=F":      "brent",
        "DX-Y.NYB":  "dxy",
        "USDRUB=X":  "usdrub",
        "AEDUSД=X":  "aedusd",
        "ES=F":      "es",
        "IMOEX.ME":  "imoex",
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

# ═══════════════════════════════════════════════
# ОТПРАВКА В TELEGRAM
# ═══════════════════════════════════════════════
def send_telegram(text, thread_id=None):
    url = "https://api.telegram.org/bot" + BOT_TOKEN + "/sendMessage"
    payload = {
        "chat_id": GROUP_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    if thread_id is not None:
        payload["message_thread_id"] = thread_id
    requests.post(url, json=payload)

# ═══════════════════════════════════════════════
# БУФЕР СИГНАЛОВ
# ═══════════════════════════════════════════════
def process_buffer():
    global signal_buffer, buffer_timer

    with buffer_lock:
        if not signal_buffer:
            return
        signals = signal_buffer.copy()
        signal_buffer = []
        buffer_timer = None

    line = "------------------------------"
    signals_header = ""

    # Определяем топик по первому сигналу
    first = signals[0]
    ticker = first.get("ticker", "")
    tf     = str(first.get("tf", "60"))
    thread_id = get_ticker_thread(ticker, tf)

    for s in signals:
        sig    = s.get("signal", "")
        ticker = s.get("ticker", "")
        price  = s.get("price", "")

        if sig == "LONG":
            emoji    = "&#128994;"   # 🟢
            sig_text = "ЛОНГ"
        elif sig == "SHORT":
            emoji    = "&#128308;"   # 🔴
            sig_text = "ШОРТ"
        elif sig == "BREAKOUT_UP":
            emoji    = "&#128640;"   # 🚀
            sig_text = "ПРОБОЙ ВВЕРХ"
        elif sig == "BREAKOUT_DOWN":
            emoji    = "&#128315;"   # 🔻
            sig_text = "ПРОБОЙ ВНИЗ"
        elif sig == "RETURN_LONG":
            emoji    = "&#128260;"   # 🔄
            sig_text = "ВОЗВРАТ ЛОНГ"
        elif sig == "RETURN_SHORT":
            emoji    = "&#128260;"   # 🔄
            sig_text = "ВОЗВРАТ ШОРТ"
        else:
            emoji    = "&#9898;"     # ⚪
            sig_text = sig or "СИГНАЛ"

        signals_header += emoji + " <b>" + sig_text + " - " + ticker + "</b>  $" + str(price) + "\n"

    count = str(len(signals))
    text = (
        "&#9889; <b>ПАКЕТ СИГНАЛОВ (" + count + ")</b>\n"
        + line + "\n"
        + signals_header
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    send_telegram(text, thread_id)

# ═══════════════════════════════════════════════
# ФОРМАТИРОВАНИЕ
# ═══════════════════════════════════════════════
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
        return str(value) + " - Альтсезон"
    elif value >= 50:
        return str(value) + " - Нейтрально"
    elif value >= 25:
        return str(value) + " - Ближе к BTC"
    else:
        return str(value) + " - Сезон BTC"

# ═══════════════════════════════════════════════
# ДАЙДЖЕСТ
# ═══════════════════════════════════════════════
def daily_report():
    btc    = get_crypto_prices()
    btc_d  = get_btc_dominance()
    total3 = get_total3()
    altseason = get_altseason_index()
    trad   = get_traditional_prices()
    fg     = get_fear_greed()
    news   = get_news()

    btc_price  = btc.get("price", 0) or 0
    btc_change = btc.get("change", 0) or 0

    gold   = trad.get("gold",   {})
    brent  = trad.get("brent",  {})
    dxy    = trad.get("dxy",    {})
    usdrub = trad.get("usdrub", {})
    aedusd = trad.get("aedusd", {})
    es     = trad.get("es",     {})
    imoex  = trad.get("imoex",  {})

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
    es_price     = es.get("price", 0) or 0
    es_change    = es.get("change", 0) or 0
    imoex_price  = imoex.get("price", 0) or 0
    imoex_change = imoex.get("change", 0) or 0
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
        + "Альтсезон (7d): " + format_altseason(altseason) + "\n"
        + line + "\n"
        + "&#127758; <b>МАКРО</b>\n"
        + "DXY: " + "{:.2f}".format(dxy_price) + " (" + "{:+.2f}".format(dxy_change) + "%)\n"
        + "&#129351; Золото: $" + "{:,.2f}".format(gold_price) + " (" + "{:+.2f}".format(gold_change) + "%)\n"
        + "&#128738; Нефть Brent: $" + "{:.2f}".format(brent_price) + " (" + "{:+.2f}".format(brent_change) + "%)\n"
        + "&#127950; ES1!: $" + "{:,.2f}".format(es_price) + " (" + "{:+.2f}".format(es_change) + "%)\n"
        + "&#127479;&#127482; IMOEX: " + "{:,.2f}".format(imoex_price) + " (" + "{:+.2f}".format(imoex_change) + "%)\n"
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
    send_telegram(text, THREAD_GENERAL)

# ═══════════════════════════════════════════════
# РОУТЫ
# ═══════════════════════════════════════════════
@app.route("/test_morning")
def test_morning():
    daily_report()
    return "Дайджест отправлен!", 200

@app.route("/test_signal")
def test_signal():
    with buffer_lock:
        signal_buffer.append({"signal": "LONG", "ticker": "BTCUSD", "price": "75000", "tf": "60"})
    process_buffer()
    return "Тестовый сигнал отправлен!", 200

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
                data = {"signal": raw, "ticker": "", "price": "", "tf": "60"}
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
