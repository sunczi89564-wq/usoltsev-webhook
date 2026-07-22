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
THREAD_1H       = 118                # сигналы 1 час
THREAD_4H       = 580                # 4 часа сигналы
THREAD_12H      = 582                # 12 часов сигналы
THREAD_1D       = 584                # 1 день сигналы
THREAD_RU       = 15                 # RU Market
THREAD_US       = 18                 # US Market
THREAD_WHALE    = 591                # Whale Alerts

# Таймфреймы → топики (для крипто)
TF_TO_THREAD = {
    "60":  THREAD_1H,
    "240": THREAD_4H,
    "720": THREAD_12H,
    "D":   THREAD_1D,
}

# Явная связка thread_id -> tf-ключ (как в TF_TO_THREAD), нужна чтобы фильтровать closed_trades по tf
THREAD_TO_TF = {
    THREAD_1H:  "60",
    THREAD_4H:  "240",
    THREAD_12H: "720",
    THREAD_1D:  "D",
}
TF_LABEL = {"60": "1ч", "240": "4ч", "720": "12ч", "D": "1д"}

signal_buffer = []
buffer_lock = threading.Lock()
buffer_timer = None
BUFFER_SECONDS = 60

# ═══════════════════════════════════════════════
# ИСТОРИЯ СДЕЛОК И СТАТИСТИКА
# ═══════════════════════════════════════════════
TRADES_FILE = "trades.json"
DEPOSIT     = 100.0
LEVERAGE    = 50
BYBIT_FEE_PCT = 0.055   # средняя комиссия тейкера Bybit за одну сторону сделки, %

# Отслеживание цен: порог хода в плюс, после которого считаем
# что стоп был передвинут в безубыток, %
BREAKEVEN_THRESHOLD_PCT = 0.3
PRICE_POLL_SECONDS = 45   # период опроса цен Bybit

def to_bybit_symbol(ticker):
    """FARTCOINUSDT.P -> FARTCOINUSDT, BTCUSD -> BTCUSDT"""
    t = ticker.upper().replace(".P", "")
    if not t.endswith("USDT") and t.endswith("USD"):
        t = t[:-3] + "USDT"
    return t

def get_bybit_price(symbol):
    try:
        url = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=" + symbol
        r = requests.get(url, timeout=8).json()
        lst = r.get("result", {}).get("list", [])
        if lst:
            return float(lst[0]["lastPrice"])
    except:
        pass
    return None

def price_tracker_loop():
    """Фоновый поток: обновляет max_price/min_price по всем открытым позициям."""
    while True:
        try:
            with trades_lock:
                data = load_trades_data()
                positions = dict(data.get("open_positions", {}))

            if positions:
                # Уникальные тикеры
                tickers = {}
                for pos_key, pos in positions.items():
                    tk = pos_key.rsplit("_", 1)[0]
                    tickers.setdefault(tk, []).append(pos_key)

                prices = {}
                for tk in tickers:
                    p = get_bybit_price(to_bybit_symbol(tk))
                    if p is not None:
                        prices[tk] = p

                if prices:
                    with trades_lock:
                        data = load_trades_data()
                        changed = False
                        for tk, keys in tickers.items():
                            p = prices.get(tk)
                            if p is None:
                                continue
                            for pos_key in keys:
                                pos = data["open_positions"].get(pos_key)
                                if pos is None:
                                    continue
                                mx = pos.get("max_price", pos["price"])
                                mn = pos.get("min_price", pos["price"])
                                if p > mx:
                                    pos["max_price"] = p
                                    changed = True
                                if p < mn:
                                    pos["min_price"] = p
                                    changed = True
                        if changed:
                            save_trades_data(data)
        except:
            pass
        time.sleep(PRICE_POLL_SECONDS)

# ═══════════════════════════════════════════════
# КИТОВЫЕ СДЕЛКИ (Whale Alerts) — через публичный поток сделок Bybit
# ═══════════════════════════════════════════════
WHALE_COINS = ["BTC", "FARTCOIN", "ETH", "LINK", "SOL", "DOGE", "PEPE", "APT",
               "SUI", "HYPE", "WIF", "AVAX", "XLM", "PUMP", "LTC", "XRP",
               "ARB", "DOT", "ATOM", "WLD", "ONDO", "C98", "TRB", "MONK"]
WHALE_THRESHOLD_USD = 25000
WHALE_POLL_SECONDS  = 25

whale_symbol_cache = {}   # coin -> реальный тикер на Bybit (или None если не нашли)
whale_seen_trades  = {}   # symbol -> set() уже отправленных execId (защита от дублей)

def resolve_whale_symbol(coin):
    """Пробует несколько вариантов написания тикера на Bybit (обычные монеты
    и мелкие с приставкой 1000, например 1000PEPEUSDT)."""
    candidates = [coin + "USDT", "1000" + coin + "USDT", "1000000" + coin + "USDT"]
    for sym in candidates:
        try:
            url = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=" + sym
            r = requests.get(url, timeout=6).json()
            lst = r.get("result", {}).get("list", [])
            if lst:
                return sym
        except:
            continue
    return None

def get_recent_trades(symbol, limit=50):
    try:
        url = "https://api.bybit.com/v5/market/recent-trade?category=linear&symbol=" + symbol + "&limit=" + str(limit)
        r = requests.get(url, timeout=8).json()
        return r.get("result", {}).get("list", [])
    except:
        return []

def send_whale_alert(coin, side, price, size, value):
    is_buy = (side or "").lower() == "buy"
    emoji = "&#128994;" if is_buy else "&#128308;"
    side_ru = "ПОКУПКА" if is_buy else "ПРОДАЖА"
    line = "------------------------------"
    text = (
        "&#128011; <b>КИТОВАЯ СДЕЛКА — " + coin + "</b>\n"
        + line + "\n"
        + emoji + " " + side_ru + ": $" + "{:,.0f}".format(value) + "\n"
        + "Цена: $" + "{:.6g}".format(price) + "\n"
        + "Объём: " + "{:.4g}".format(size) + " " + coin + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    send_telegram(text, THREAD_WHALE)

def whale_tracker_loop():
    """Фоновый поток: опрашивает последние сделки по списку монет,
    шлёт алерт если одна сделка превышает WHALE_THRESHOLD_USD."""
    # один раз находим реальные тикеры Bybit для каждой монеты
    for coin in WHALE_COINS:
        whale_symbol_cache[coin] = resolve_whale_symbol(coin)
        time.sleep(0.3)

    while True:
        try:
            for coin in WHALE_COINS:
                sym = whale_symbol_cache.get(coin)
                if not sym:
                    continue
                trades = get_recent_trades(sym)
                seen = whale_seen_trades.setdefault(sym, set())
                for t in trades:
                    exec_id = t.get("execId") or t.get("i")
                    if not exec_id or exec_id in seen:
                        continue
                    seen.add(exec_id)
                    try:
                        price = float(t.get("price", 0))
                        size  = float(t.get("size", 0))
                    except:
                        continue
                    value = price * size
                    if value >= WHALE_THRESHOLD_USD:
                        send_whale_alert(coin, t.get("side", ""), price, size, value)
                if len(seen) > 500:
                    whale_seen_trades[sym] = set(list(seen)[-250:])
        except:
            pass
        time.sleep(WHALE_POLL_SECONDS)

trades_lock = threading.Lock()

def load_trades_data():
    if os.path.exists(TRADES_FILE):
        try:
            with open(TRADES_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {"start_date": None, "open_positions": {}, "closed_trades": []}

def save_trades_data(data):
    try:
        with open(TRADES_FILE, "w") as f:
            json.dump(data, f)
    except:
        pass

def f_price(p):
    try:
        return float(p)
    except:
        return None

def process_trade_signal(ticker, sig, price, tf="60"):
    """Обрабатывает LONG/SHORT сигнал: закрывает предыдущую позицию по тикеру+таймфрейму
    (если была) и открывает новую. Позиции 5м и 15м по одному тикеру независимы.
    Возвращает текст сравнения для вставки в сообщение, либо None если сравнивать не с чем."""
    if sig not in ("LONG", "SHORT"):
        return None
    price_val = f_price(price)
    if price_val is None:
        return None

    pos_key = ticker + "_" + str(tf)

    with trades_lock:
        data = load_trades_data()
        if data["start_date"] is None:
            data["start_date"] = datetime.now().strftime("%d.%m.%Y")

        compare_text = None
        prev = data["open_positions"].get(pos_key)

        if prev is not None:
            prev_sig   = prev.get("signal")
            prev_price = prev.get("price")
            if prev_price:
                change_pct = (price_val - prev_price) / prev_price * 100
                if prev_sig == "LONG":
                    pnl_pct = change_pct
                else:
                    pnl_pct = -change_pct

                position_size = DEPOSIT * LEVERAGE
                fee_usd = position_size * (BYBIT_FEE_PCT / 100) * 2   # вход + выход
                is_same_direction = (prev_sig == sig)
                breakeven_note = ""

                # Реальный максимальный ход в пользу позиции (из отслеживания цен)
                mx = prev.get("max_price", prev_price)
                mn = prev.get("min_price", prev_price)
                if prev_sig == "LONG":
                    favorable_pct = (mx - prev_price) / prev_price * 100
                else:
                    favorable_pct = (prev_price - mn) / prev_price * 100

                if is_same_direction and favorable_pct >= BREAKEVEN_THRESHOLD_PCT:
                    # Цена реально ходила в плюс >= порога — стоп был в безубытке
                    pnl_usd = -fee_usd
                    breakeven_note = " (безубыток, макс ход +" + "{:.2f}".format(favorable_pct) + "%)"
                else:
                    pnl_usd = position_size * (pnl_pct / 100) - fee_usd
                    if favorable_pct > 0:
                        breakeven_note = " (макс ход +" + "{:.2f}".format(favorable_pct) + "%)"

                result_emoji = "&#9989;" if pnl_usd >= 0 else "&#10060;"
                reversal = " | &#128260; Разворот" if prev_sig != sig else ""

                compare_text = (
                    "&#8618; Пред: " + ("ЛОНГ" if prev_sig == "LONG" else "ШОРТ")
                    + " $" + str(prev_price)
                    + " (" + "{:+.2f}".format(pnl_pct) + "%, " + "{:+.2f}".format(pnl_usd) + "$ после комиссии" + breakeven_note + ") "
                    + result_emoji + reversal
                )

                data["closed_trades"].append({
                    "ticker": ticker,
                    "signal": prev_sig,
                    "open_price": prev_price,
                    "close_price": price_val,
                    "pnl_pct": pnl_pct,
                    "pnl_usd": pnl_usd,
                    "tf": str(tf),
                    "closed_at": datetime.now().isoformat()
                })
                # Ограничиваем историю последними 2000 сделками
                if len(data["closed_trades"]) > 2000:
                    data["closed_trades"] = data["closed_trades"][-2000:]

        data["open_positions"][pos_key] = {"signal": sig, "price": price_val, "tf": str(tf), "max_price": price_val, "min_price": price_val, "opened_at": datetime.now().isoformat()}
        save_trades_data(data)

    return compare_text

def process_return_signal(ticker, sig, price, tf="60"):
    """Возврат в канал как сигнал ЗАКРЫТИЯ противоположной позиции:
    RETURN_SHORT (импульс вверх выдохся) закрывает открытый LONG,
    RETURN_LONG (импульс вниз выдохся) закрывает открытый SHORT.
    Позиция закрывается с реальным P&L, новая не открывается.
    Работает в рамках того же таймфрейма (ключ ticker_tf)."""
    if sig not in ("RETURN_LONG", "RETURN_SHORT"):
        return None
    price_val = f_price(price)
    if price_val is None:
        return None

    close_direction = "SHORT" if sig == "RETURN_LONG" else "LONG"
    pos_key = ticker + "_" + str(tf)

    with trades_lock:
        data = load_trades_data()
        prev = data["open_positions"].get(pos_key)
        if prev is None or prev.get("signal") != close_direction:
            return None

        prev_price = prev.get("price")
        if not prev_price:
            return None

        change_pct = (price_val - prev_price) / prev_price * 100
        pnl_pct = change_pct if close_direction == "LONG" else -change_pct

        position_size = DEPOSIT * LEVERAGE
        fee_usd = position_size * (BYBIT_FEE_PCT / 100) * 2
        pnl_usd = position_size * (pnl_pct / 100) - fee_usd

        result_emoji = "&#9989;" if pnl_usd >= 0 else "&#10060;"
        compare_text = (
            "&#128274; Закрыт " + ("ЛОНГ" if close_direction == "LONG" else "ШОРТ")
            + " $" + str(prev_price)
            + " (" + "{:+.2f}".format(pnl_pct) + "%, " + "{:+.2f}".format(pnl_usd) + "$ после комиссии) "
            + result_emoji
        )

        data["closed_trades"].append({
            "ticker": ticker,
            "signal": close_direction,
            "open_price": prev_price,
            "close_price": price_val,
            "pnl_pct": pnl_pct,
            "pnl_usd": pnl_usd,
            "closed_by": "return",
            "tf": str(tf),
            "closed_at": datetime.now().isoformat()
        })
        if len(data["closed_trades"]) > 2000:
            data["closed_trades"] = data["closed_trades"][-2000:]

        del data["open_positions"][pos_key]
        save_trades_data(data)

    return compare_text

def calc_stats(period_hours, tf_filter=None):
    with trades_lock:
        data = load_trades_data()
    trades = data.get("closed_trades", [])
    start_date = data.get("start_date", "—")

    cutoff = datetime.now().timestamp() - period_hours * 3600
    filtered = []
    for t in trades:
        try:
            ts = datetime.fromisoformat(t["closed_at"]).timestamp()
            if ts < cutoff:
                continue
            if tf_filter is not None and t.get("tf") != tf_filter:
                continue
            filtered.append(t)
        except:
            continue

    total = len(filtered)
    wins  = sum(1 for t in filtered if t["pnl_usd"] >= 0)
    losses = total - wins
    winrate = (wins / total * 100) if total > 0 else 0
    total_pnl = sum(t["pnl_usd"] for t in filtered)

    ret_trades = [t for t in filtered if t.get("closed_by") == "return"]
    ret_total  = len(ret_trades)
    ret_wins   = sum(1 for t in ret_trades if t["pnl_usd"] >= 0)
    ret_winrate = (ret_wins / ret_total * 100) if ret_total > 0 else 0
    ret_pnl    = sum(t["pnl_usd"] for t in ret_trades)

    return {
        "total": total,
        "wins": wins,
        "losses": losses,
        "winrate": winrate,
        "total_pnl": total_pnl,
        "ret_total": ret_total,
        "ret_winrate": ret_winrate,
        "ret_pnl": ret_pnl,
        "start_date": start_date
    }

def format_stats(period_name, period_hours, tf_filter=None):
    s = calc_stats(period_hours, tf_filter)
    line = "------------------------------"
    pnl_emoji = "&#128200;" if s["total_pnl"] >= 0 else "&#128201;"
    tf_note = " (" + TF_LABEL.get(tf_filter, tf_filter) + ")" if tf_filter else " (все ТФ)"
    text = (
        "&#128202; <b>СТАТИСТИКА — " + period_name + tf_note + "</b>\n"
        + line + "\n"
        + "Сделок: " + str(s["total"]) + "\n"
        + "&#9989; Профит: " + str(s["wins"]) + "  &#10060; Убыток: " + str(s["losses"]) + "\n"
        + "Винрейт: " + "{:.1f}".format(s["winrate"]) + "%\n"
        + pnl_emoji + " P&amp;L: " + "{:+.2f}".format(s["total_pnl"]) + "$\n"
        + "&#128274; Закрыто по возврату: " + str(s["ret_total"]) + " (винрейт " + "{:.1f}".format(s["ret_winrate"]) + "%, " + "{:+.2f}".format(s["ret_pnl"]) + "$)\n"
        + line + "\n"
        + "Депозит: $" + str(int(DEPOSIT)) + " | Плечо: x" + str(LEVERAGE) + " | Комиссия Bybit учтена (0.055% x2)\n"
        + "Статистика ведётся с: " + s["start_date"] + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    return text

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
    return TF_TO_THREAD.get(str(tf), THREAD_1H)

# ═══════════════════════════════════════════════
# КРИПТО ДАННЫЕ
# ═══════════════════════════════════════════════
def get_crypto_prices():
    # Основной источник: CryptoCompare
    try:
        url = "https://min-api.cryptocompare.com/data/pricemultifull?fsyms=BTC&tsyms=USD"
        r = requests.get(url, timeout=10).json()
        raw = r["RAW"]
        price = raw["BTC"]["USD"]["PRICE"]
        change = raw["BTC"]["USD"]["CHANGEPCT24HOUR"]
        if price and price > 0:
            return {"price": price, "change": change}
    except:
        pass
    # Резерв: CoinGecko simple/price
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true"
        r = requests.get(url, timeout=10).json()
        btc = r.get("bitcoin", {})
        price = btc.get("usd", 0)
        change = btc.get("usd_24h_change", 0) or 0
        if price and price > 0:
            return {"price": price, "change": change}
    except:
        pass
    # Резерв 2: Bybit (у нас уже есть функция)
    try:
        p = get_bybit_price("BTCUSDT")
        if p:
            return {"price": p, "change": 0}
    except:
        pass
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
def get_imoex():
    """Индекс Мосбиржи через официальный ISS API (Yahoo нестабилен с IMOEX.ME).
    Не гадаем имена колонок заранее — запрашиваем блок целиком и находим
    нужные поля по фактическому списку columns, который вернул MOEX."""
    try:
        url = "https://iss.moex.com/iss/engines/stock/markets/index/securities/IMOEX.json?iss.meta=off&iss.only=marketdata"
        r = requests.get(url, timeout=8).json()
        md = r.get("marketdata", {})
        cols = md.get("columns", [])
        rows = md.get("data", [])
        if not rows or not cols:
            return {"price": 0, "change": 0}

        row = rows[0]

        def col_idx(*names):
            for n in names:
                if n in cols:
                    return cols.index(n)
            return None

        price_i  = col_idx("CURRENTVALUE", "LASTVALUE")
        change_i = col_idx("LASTCHANGEPRC", "LASTCHANGEPRCNT", "CHANGE", "LASTTOPREVPRICE")

        price  = float(row[price_i])  if price_i  is not None and row[price_i]  is not None else 0
        change = float(row[change_i]) if change_i is not None and row[change_i] is not None else 0

        return {"price": round(price, 2), "change": round(change, 2)}
    except:
        return {"price": 0, "change": 0}

def get_traditional_prices():
    results = {}
    pairs = {
        "GC=F":      "gold",
        "BZ=F":      "brent",
        "DX-Y.NYB":  "dxy",
        "USDRUB=X":  "usdrub",
        "AEDUSД=X":  "aedusd",
        "ES=F":      "es",
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

    # Пробои не публикуем в Telegram (решили по итогам 3 недель наблюдений) —
    # индикатор их по-прежнему считает, просто не шлём в чат
    SKIP_SIGNALS = ("BREAKOUT_UP", "BREAKOUT_DOWN")
    signals = [s for s in signals if s.get("signal", "") not in SKIP_SIGNALS]

    if not signals:
        return

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

        target = s.get("target", "")
        stop   = s.get("stop", "")
        if target and stop:
            try:
                target_f = float(target)
                stop_f   = float(stop)
                signals_header += (
                    "&#127919; Цель: $" + "{:.6g}".format(target_f)
                    + "  &#128721; Стоп: $" + "{:.6g}".format(stop_f) + "\n"
                )
            except:
                pass

        sig_tf = str(s.get("tf", "60"))
        compare = process_trade_signal(ticker, sig, price, sig_tf)
        if compare:
            signals_header += compare + "\n"

        return_close = process_return_signal(ticker, sig, price, sig_tf)
        if return_close:
            signals_header += return_close + "\n"

    signal_count = len(signals)
    if signal_count > 1:
        text = (
            "&#9889; <b>ПАКЕТ СИГНАЛОВ (" + str(signal_count) + ")</b>\n"
            + line + "\n"
            + signals_header
            + line + "\n"
            + "<i>Usoltsev Signals</i>"
        )
    else:
        text = (
            signals_header
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
    imoex  = get_imoex()

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

@app.route("/test_whale")
def test_whale():
    send_whale_alert("BTC", "Buy", 65000, 1.5, 97500)
    return "Тестовый китовый алерт отправлен!", 200

@app.route("/test_onchain")
def test_onchain():
    """Диагностика: запускает один цикл сканирования сразу и показывает что реально
    произошло (без try/except pass — чтобы увидеть настоящие ошибки, если они есть)."""
    report = {}

    report["etherscan_key_set"] = bool(ETHERSCAN_API_KEY)
    report["solscan_key_set"] = bool(SOLSCAN_API_KEY)

    try:
        refresh_onchain_universe()
        report["universe_eth_count"] = len(onchain_universe.get("eth", {}))
        report["universe_sol_count"] = len(onchain_universe.get("sol", {}))
        report["universe_eth_sample"] = list(onchain_universe.get("eth", {}).items())[:3]
        report["universe_sol_sample"] = list(onchain_universe.get("sol", {}).items())[:3]
    except Exception as e:
        report["universe_error"] = str(e)

    # Пробуем один реальный запрос к Etherscan (WBTC — стабильный, всегда есть в FIXED_ETH_TOKENS)
    try:
        wbtc_addr, wbtc_dec = FIXED_ETH_TOKENS["WBTC"]
        url = ("https://api.etherscan.io/v2/api?chainid=" + str(ETH_CHAIN_ID)
               + "&module=account&action=tokentx&contractaddress=" + wbtc_addr
               + "&sort=desc&page=1&offset=3&apikey=" + str(ETHERSCAN_API_KEY))
        r = requests.get(url, timeout=10).json()
        report["etherscan_test_status"] = r.get("status")
        report["etherscan_test_message"] = r.get("message")
        result = r.get("result")
        report["etherscan_test_result_sample"] = result[:1] if isinstance(result, list) else result
    except Exception as e:
        report["etherscan_test_error"] = str(e)

    # Пробуем один реальный запрос к Solscan
    try:
        sol_sample = list(onchain_universe.get("sol", {}).items())
        if sol_sample:
            sym, info = sol_sample[0]
            url = ("https://pro-api.solscan.io/v2.0/token/transfer?address=" + info["contract"]
                   + "&page=1&page_size=3&sort_by=block_time&sort_order=desc")
            headers = {"token": SOLSCAN_API_KEY}
            r = requests.get(url, headers=headers, timeout=10).json()
            report["solscan_test_symbol"] = sym
            report["solscan_test_response"] = r
        else:
            report["solscan_test_note"] = "нет ни одного Solana-токена в universe для теста"
    except Exception as e:
        report["solscan_test_error"] = str(e)

    return json.dumps(report, indent=2, ensure_ascii=False, default=str), 200, {"Content-Type": "application/json; charset=utf-8"}

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

# ═══════════════════════════════════════════════
# TELEGRAM BOT COMMANDS (webhook на входящие сообщения)
# ═══════════════════════════════════════════════
@app.route("/telegram_updates", methods=["POST"])
def telegram_updates():
    try:
        update = request.json or {}
    except:
        update = {}

    msg = update.get("message") or update.get("channel_post")
    if not msg:
        return "OK", 200

    text      = (msg.get("text") or "").strip()
    thread_id = msg.get("message_thread_id")
    tf_filter = THREAD_TO_TF.get(thread_id)  # None если тема не привязана к ТФ (например General)

    if text.startswith("/stats24h"):
        send_telegram(format_stats("24 ЧАСА", 24, tf_filter), thread_id)
    elif text.startswith("/statsweek"):
        send_telegram(format_stats("НЕДЕЛЯ", 24 * 7, tf_filter), thread_id)
    elif text.startswith("/statsmonth"):
        send_telegram(format_stats("МЕСЯЦ", 24 * 30, tf_filter), thread_id)

    return "OK", 200

@app.route("/set_telegram_webhook")
def set_telegram_webhook():
    webhook_url = "https://usoltsev-webhook-production.up.railway.app/telegram_updates"
    tg_url = "https://api.telegram.org/bot" + BOT_TOKEN + "/setWebhook"
    r = requests.post(tg_url, json={"url": webhook_url})
    return r.text, 200

@app.route("/")
def index():
    return "Webhook работает!", 200

scheduler = BackgroundScheduler()
scheduler.add_job(daily_report, "cron", hour=4,  minute=0)
scheduler.add_job(daily_report, "cron", hour=10, minute=0)
scheduler.add_job(daily_report, "cron", hour=14, minute=0)
scheduler.start()

# ═══════════════════════════════════════════════
# ОНЧЕЙН КИТЫ (спотовые закупки, Ethereum + Solana)
# ═══════════════════════════════════════════════
ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY")
SOLSCAN_API_KEY   = os.environ.get("SOLSCAN_API_KEY")
# Онлайн-киты идут в ту же тему, что и биржевые киты — THREAD_WHALE (591), уже объявлена выше

ONCHAIN_USD_THRESHOLD        = 50000
ONCHAIN_TOP_N                = 120     # сколько топ-монет по объёму брать с CoinGecko
ONCHAIN_UNIVERSE_REFRESH_SEC = 3600    # как часто обновлять список топ-монет
ONCHAIN_POLL_SEC             = 180     # как часто сканировать на предмет крупных переводов

ETH_CHAIN_ID  = 1
AVAX_CHAIN_ID = 43114
HYPE_CHAIN_ID = 999

# Фиксированные токены, которые мы уже торгуем — добавлены всегда, независимо от топ-N
FIXED_ETH_TOKENS = {
    # symbol: (contract_address, decimals)
    "LINK": ("0x514910771af9ca656af840dff83e8264ecf986ca", 18),
    "PEPE": ("0x6982508145454ce325ddbe47a25d4ec3d2311933", 18),
    "ARB":  ("0xb50721bcf8d664c30412cfbc6cf7a15145234ad1", 18),
    "WLD":  ("0x163f8c2467924be0ae7b5347228d17d92f5cfffd", 18),
    "ONDO": ("0xfaba6f8e4a5e8ab82f62fe7c39859fa577269be3", 18),
    "TRB":  ("0x88df592f8eb5d7bd38bfef7deb0fbc02cf3778a0", 18),
    "C98":  ("0xaec945e04baf28b135fa7c640f624f8d90f1c3a6", 18),
    "WBTC": ("0x2260fac5e5542a773aa44fbcfedf7c193bc2c599", 8),
}

# Известные адреса крупных бирж — НАМЕРЕННО короткий список, только те что уверенно проверены.
# Расширять только реально сверенными адресами (например через label на Etherscan/Solscan),
# неправильный адрес тут исказит сигнал купли/продажи.
KNOWN_EXCHANGE_ETH = {
    "0x28c6c06298d514db089934071355e5743bf21d60": "Binance",
}
KNOWN_EXCHANGE_SOL = {}   # пока пусто — добавим когда появятся проверенные адреса

onchain_universe = {"eth": {}, "sol": {}, "updated": 0}   # symbol -> {"contract":.., "decimals":.., "price":..}
onchain_seen = set()   # уже отправленные tx hash, с капом

def refresh_onchain_universe():
    """Раз в час: топ-N монет по объёму с CoinGecko + их адреса контрактов на Ethereum/Solana."""
    try:
        list_url = "https://api.coingecko.com/api/v3/coins/list?include_platform=true"
        platforms = requests.get(list_url, timeout=20).json()
        plat_by_id = {c["id"]: c.get("platforms", {}) for c in platforms if "id" in c}

        mkt_url = ("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd"
                   "&order=volume_desc&per_page=" + str(ONCHAIN_TOP_N) + "&page=1")
        markets = requests.get(mkt_url, timeout=20).json()

        eth_map = {}
        sol_map = {}
        for coin in markets:
            cid = coin.get("id")
            price = coin.get("current_price", 0) or 0
            plats = plat_by_id.get(cid, {})
            symbol = (coin.get("symbol") or "").upper()
            eth_addr = plats.get("ethereum")
            sol_addr = plats.get("solana")
            if eth_addr:
                eth_map[symbol] = {"contract": eth_addr.lower(), "price": price}
            if sol_addr:
                sol_map[symbol] = {"contract": sol_addr, "price": price}

        # добавляем фиксированные токены (цену возьмём отдельно при сканировании, если не нашлась выше)
        for sym, (addr, dec) in FIXED_ETH_TOKENS.items():
            if sym not in eth_map:
                eth_map[sym] = {"contract": addr, "price": 0}

        onchain_universe["eth"] = eth_map
        onchain_universe["sol"] = sol_map
        onchain_universe["updated"] = time.time()
    except:
        pass

def get_token_price_fallback(symbol):
    try:
        url = "https://api.coingecko.com/api/v3/simple/price?ids=" + symbol.lower() + "&vs_currencies=usd"
        r = requests.get(url, timeout=8).json()
        for v in r.values():
            return v.get("usd", 0)
    except:
        pass
    return 0

def send_onchain_alert(symbol, chain_name, tx_hash, amount, usd_value, from_addr, to_addr):
    from_tag = KNOWN_EXCHANGE_ETH.get(from_addr, KNOWN_EXCHANGE_SOL.get(from_addr, ""))
    to_tag   = KNOWN_EXCHANGE_ETH.get(to_addr,   KNOWN_EXCHANGE_SOL.get(to_addr, ""))

    if to_tag:
        direction = "&#128308; На биржу (" + to_tag + ") — возможна продажа"
    elif from_tag:
        direction = "&#128994; С биржи (" + from_tag + ") — возможное накопление"
    else:
        direction = "&#9898; Кошелёк ↔ кошелёк (биржа не определена)"

    line = "------------------------------"
    text = (
        "&#128011; <b>ОНЧЕЙН КИТ — " + symbol + "</b> (" + chain_name + ")\n"
        + line + "\n"
        + "Сумма: $" + "{:,.0f}".format(usd_value) + "  (" + "{:.4g}".format(amount) + " " + symbol + ")\n"
        + direction + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    send_telegram(text, THREAD_WHALE)

def scan_eth_token(symbol, contract, chainid=ETH_CHAIN_ID, price=0):
    if not ETHERSCAN_API_KEY:
        return
    try:
        url = ("https://api.etherscan.io/v2/api?chainid=" + str(chainid)
               + "&module=account&action=tokentx&contractaddress=" + contract
               + "&sort=desc&page=1&offset=20&apikey=" + ETHERSCAN_API_KEY)
        r = requests.get(url, timeout=10).json()
        rows = r.get("result", [])
        if not isinstance(rows, list):
            return
        px = price if price and price > 0 else get_token_price_fallback(symbol)
        if not px:
            return
        for row in rows:
            tx_hash = row.get("hash")
            if not tx_hash or tx_hash in onchain_seen:
                continue
            try:
                dec = int(row.get("tokenDecimal", 18))
                raw_val = float(row.get("value", 0))
                amount = raw_val / (10 ** dec)
            except:
                continue
            usd_value = amount * px
            if usd_value >= ONCHAIN_USD_THRESHOLD:
                onchain_seen.add(tx_hash)
                send_onchain_alert(symbol, "ETH", tx_hash, amount, usd_value,
                                    (row.get("from") or "").lower(), (row.get("to") or "").lower())
    except:
        pass

def scan_sol_token(symbol, mint, price=0):
    if not SOLSCAN_API_KEY:
        return
    try:
        url = "https://pro-api.solscan.io/v2.0/token/transfer?address=" + mint + "&page=1&page_size=20&sort_by=block_time&sort_order=desc"
        headers = {"token": SOLSCAN_API_KEY}
        r = requests.get(url, headers=headers, timeout=10).json()
        rows = r.get("data", [])
        if not isinstance(rows, list):
            return
        px = price if price and price > 0 else get_token_price_fallback(symbol)
        if not px:
            return
        for row in rows:
            tx_hash = row.get("trans_id") or row.get("tx_hash")
            if not tx_hash or tx_hash in onchain_seen:
                continue
            try:
                amount = float(row.get("amount", 0)) / (10 ** int(row.get("token_decimal", 0)))
            except:
                continue
            usd_value = amount * px
            if usd_value >= ONCHAIN_USD_THRESHOLD:
                onchain_seen.add(tx_hash)
                send_onchain_alert(symbol, "SOL", tx_hash, amount, usd_value,
                                    row.get("from_address", ""), row.get("to_address", ""))
    except:
        pass

def scan_known_exchange_native(chainid, chain_name, symbol):
    """Для нативных монет (ETH/AVAX/HYPE) — только известные адреса бирж, а не вся сеть."""
    if not ETHERSCAN_API_KEY or not KNOWN_EXCHANGE_ETH:
        return
    px = get_token_price_fallback(symbol)
    if not px:
        return
    for addr, tag in KNOWN_EXCHANGE_ETH.items():
        try:
            url = ("https://api.etherscan.io/v2/api?chainid=" + str(chainid)
                   + "&module=account&action=txlist&address=" + addr
                   + "&sort=desc&page=1&offset=10&apikey=" + ETHERSCAN_API_KEY)
            r = requests.get(url, timeout=10).json()
            rows = r.get("result", [])
            if not isinstance(rows, list):
                continue
            for row in rows:
                tx_hash = row.get("hash")
                if not tx_hash or tx_hash in onchain_seen:
                    continue
                try:
                    amount = float(row.get("value", 0)) / (10 ** 18)
                except:
                    continue
                usd_value = amount * px
                if usd_value >= ONCHAIN_USD_THRESHOLD:
                    onchain_seen.add(tx_hash)
                    send_onchain_alert(symbol, chain_name, tx_hash, amount, usd_value,
                                        (row.get("from") or "").lower(), (row.get("to") or "").lower())
        except:
            continue

def onchain_whale_loop():
    refresh_onchain_universe()
    last_universe_refresh = time.time()
    while True:
        try:
            if time.time() - last_universe_refresh >= ONCHAIN_UNIVERSE_REFRESH_SEC:
                refresh_onchain_universe()
                last_universe_refresh = time.time()

            for sym, info in list(onchain_universe.get("eth", {}).items()):
                scan_eth_token(sym, info["contract"], ETH_CHAIN_ID, info.get("price", 0))

            for sym, info in list(onchain_universe.get("sol", {}).items()):
                scan_sol_token(sym, info["contract"], info.get("price", 0))

            scan_known_exchange_native(ETH_CHAIN_ID, "ETH", "ETH")
            scan_known_exchange_native(AVAX_CHAIN_ID, "AVAX", "AVAX")
            scan_known_exchange_native(HYPE_CHAIN_ID, "HYPE", "HYPE")

            # ограничиваем множество уже отправленных tx, чтобы не росло бесконечно
            if len(onchain_seen) > 5000:
                onchain_seen.clear()
        except:
            pass
        time.sleep(ONCHAIN_POLL_SEC)

# Фоновый поток отслеживания цен Bybit
price_thread = threading.Thread(target=price_tracker_loop, daemon=True)
price_thread.start()

# Фоновый поток отслеживания китовых сделок (крупные сделки на бирже)
whale_thread = threading.Thread(target=whale_tracker_loop, daemon=True)
whale_thread.start()

# Фоновый поток ончейн-китов (спотовые закупки Ethereum + Solana)
onchain_thread = threading.Thread(target=onchain_whale_loop, daemon=True)
onchain_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
