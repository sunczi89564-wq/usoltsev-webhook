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
THREAD_VOLUME   = 1092               # Volume Alert (всплески среднего объёма по нашим тикерам)

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
WHALE_THRESHOLD_USD = 100000
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

# Буфер для группировки китовых алертов (биржевых и ончейн) — отправляем одной сводкой раз в 30 минут.
# Каждая запись — структурированный словарь (не готовая строка), чтобы flush_whale_alerts
# мог сгруппировать по монете и категории перед отправкой.
whale_alert_lock = threading.Lock()
whale_alert_buffer = []   # список словарей {"coin":.., "category":.., "value":.., "chain":..}

# Категории движения:
#   "buy"      — накопление (приход с биржи на кошелёк / покупка на бирже)
#   "sell"     — продажа (уход на биржу / продажа на бирже)
#   "transfer" — кошелёк↔кошелёк, без привязки к бирже (собирается, но НЕ показывается в сводке — слишком шумно)
WHALE_CATEGORY_LABEL = {
    "buy":      "&#128994; Накопление",
    "sell":     "&#128308; Продажа",
    "transfer": "&#9898; Кошелёк↔кошелёк",
}

def send_whale_alert(coin, side, price, size, value):
    """Биржевая сделка Bybit: покупка на бирже = buy, продажа = sell."""
    is_buy = (side or "").lower() == "buy"
    category = "buy" if is_buy else "sell"
    with whale_alert_lock:
        whale_alert_buffer.append({"coin": coin, "category": category, "value": value, "chain": "ETH"})

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
    Возвращает текст сравнения (используется только для внутреннего учёта / статистики,
    в чат в основном сообщении сигнала БОЛЬШЕ НЕ ВСТАВЛЯЕТСЯ — см. process_buffer)."""
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
    Работает в рамках того же таймфрейма (ключ ticker_tf).
    Возвращаемый текст используется только для внутреннего учёта / статистики,
    в чат в основном сообщении сигнала БОЛЬШЕ НЕ ВСТАВЛЯЕТСЯ — см. process_buffer."""
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
        # Функции по-прежнему вызываются и пишут в trades.json (нужно для /stats24h,
        # /statsweek, /statsmonth), но возвращаемый текст сравнения (P&L, ✅/❌,
        # "Пред: ЛОНГ/ШОРТ...", "Закрыт ЛОНГ/ШОРТ...") в сообщение больше НЕ добавляется —
        # в чат идёт только сам сигнал (тип, тикер, цена), без расчёта заработка.
        process_trade_signal(ticker, sig, price, sig_tf)
        process_return_signal(ticker, sig, price, sig_tf)

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
    flush_whale_alerts()
    return "Тестовый китовый алерт отправлен (сводкой)!", 200

@app.route("/test_bybit_raw")
def test_bybit_raw():
    """Честная диагностика связи с Bybit: ОДИН запрос на тикер BTCUSDT (без резолвера,
    без цикла по 24 монетам, без try/except-заглушек). Показывает точный HTTP-статус,
    заголовки ответа и тело — чтобы отличить geo-block, rate-limit, таймаут или
    настоящую пустоту в ответе."""
    report = {}

    # 1) Тикеры (тот же эндпоинт, что использует resolve_whale_symbol)
    try:
        url = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=BTCUSDT"
        resp = requests.get(url, timeout=10)
        report["tickers_http_status"] = resp.status_code
        report["tickers_headers_sample"] = {
            "content-type": resp.headers.get("content-type"),
            "cf-ray": resp.headers.get("cf-ray"),          # если есть — запрос дошёл до Cloudflare Bybit
            "x-bapi-limit": resp.headers.get("x-bapi-limit"),
        }
        report["tickers_raw_text_first_500"] = resp.text[:500]
        try:
            report["tickers_json"] = resp.json()
        except Exception as e:
            report["tickers_json_parse_error"] = str(e)
    except requests.exceptions.Timeout:
        report["tickers_error"] = "TIMEOUT — запрос не получил ответ за 10 секунд (похоже на geo-block/фаервол)"
    except requests.exceptions.ConnectionError as e:
        report["tickers_error"] = "CONNECTION ERROR: " + str(e)
    except Exception as e:
        report["tickers_error"] = "OTHER: " + type(e).__name__ + ": " + str(e)

    # 2) Kline (тот же эндпоинт, что использует get_hourly_klines)
    try:
        url2 = "https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT&interval=60&limit=3"
        resp2 = requests.get(url2, timeout=10)
        report["kline_http_status"] = resp2.status_code
        report["kline_raw_text_first_500"] = resp2.text[:500]
        try:
            report["kline_json"] = resp2.json()
        except Exception as e:
            report["kline_json_parse_error"] = str(e)
    except requests.exceptions.Timeout:
        report["kline_error"] = "TIMEOUT — запрос не получил ответ за 10 секунд (похоже на geo-block/фаервол)"
    except requests.exceptions.ConnectionError as e:
        report["kline_error"] = "CONNECTION ERROR: " + str(e)
    except Exception as e:
        report["kline_error"] = "OTHER: " + type(e).__name__ + ": " + str(e)

    # 3) Внешний IP контейнера Railway — если Bybit блокирует по гео, полезно знать откуда вообще стучимся
    try:
        ip_resp = requests.get("https://api.ipify.org?format=json", timeout=6)
        report["outbound_ip"] = ip_resp.json().get("ip")
    except Exception as e:
        report["outbound_ip_error"] = str(e)

    return json.dumps(report, indent=2, ensure_ascii=False, default=str), 200, {"Content-Type": "application/json; charset=utf-8"}

@app.route("/test_volume")
def test_volume():
    """Разовый принудительный проход по всем монетам без ожидания и БЕЗ учёта
    гистерезиса — полезно для проверки что запрос к Bybit kline работает."""
    report = []
    for coin in WHALE_COINS:
        sym = whale_symbol_cache.get(coin) or resolve_whale_symbol(coin)
        whale_symbol_cache[coin] = sym
        if not sym:
            report.append({"coin": coin, "error": "symbol not resolved"})
            continue
        rows = get_hourly_klines(sym, VOL_AVG_BARS + 2)
        if len(rows) < VOL_AVG_BARS + 2:
            report.append({"coin": coin, "symbol": sym, "error": "not enough klines", "got": len(rows)})
            continue
        last_closed = rows[1]
        avg_rows = rows[2:2 + VOL_AVG_BARS]
        try:
            cur_vol = float(last_closed[5])
            avg_vol = sum(float(r[5]) for r in avg_rows) / len(avg_rows) if avg_rows else 0
            ratio = cur_vol / avg_vol if avg_vol > 0 else 0
        except Exception as e:
            report.append({"coin": coin, "symbol": sym, "error": str(e)})
            continue
        report.append({"coin": coin, "symbol": sym, "ratio": round(ratio, 2)})
    return json.dumps(report, indent=2, ensure_ascii=False), 200, {"Content-Type": "application/json; charset=utf-8"}

@app.route("/test_volume_loop_once")
def test_volume_loop_once():
    """Выполняет РОВНО ту же логику, что один проход volume_alert_loop —
    но синхронно, БЕЗ try/except, и с реальной отправкой в Telegram (THREAD_VOLUME),
    полностью игнорируя гистерезис (для теста считаем все монеты 'armed').
    Если тут будет исключение — оно вылезет прямо в браузер как traceback,
    а не потеряется в фоновом потоке."""
    hits = []
    debug_rows = []
    for coin in WHALE_COINS:
        sym = whale_symbol_cache.get(coin) or resolve_whale_symbol(coin)
        whale_symbol_cache[coin] = sym
        if not sym:
            debug_rows.append({"coin": coin, "skip": "symbol not resolved"})
            continue
        rows = get_hourly_klines(sym, VOL_AVG_BARS + 2)
        if len(rows) < VOL_AVG_BARS + 2:
            debug_rows.append({"coin": coin, "symbol": sym, "skip": "not enough klines", "got": len(rows)})
            continue
        last_closed = rows[1]
        avg_rows = rows[2:2 + VOL_AVG_BARS]
        if len(avg_rows) < VOL_AVG_BARS:
            debug_rows.append({"coin": coin, "symbol": sym, "skip": "not enough avg_rows"})
            continue
        cur_vol = float(last_closed[5])
        cur_close = float(last_closed[4])
        cur_open = float(last_closed[1])
        avg_vol = sum(float(r[5]) for r in avg_rows) / len(avg_rows)
        if avg_vol <= 0:
            debug_rows.append({"coin": coin, "symbol": sym, "skip": "avg_vol <= 0"})
            continue
        ratio = cur_vol / avg_vol
        debug_rows.append({"coin": coin, "symbol": sym, "ratio": round(ratio, 2)})
        if ratio >= VOL_ALERT_RATIO:
            cur_vol_usd = cur_vol * cur_close
            avg_vol_usd = avg_vol * cur_close
            is_green = cur_close >= cur_open
            hits.append((coin, ratio, cur_vol_usd, avg_vol_usd, is_green))

    sent = False
    if hits:
        hits.sort(key=lambda h: h[1], reverse=True)
        lines = []
        for coin, ratio, cur_usd, avg_usd, is_green in hits:
            dir_emoji = "&#128994;" if is_green else "&#128308;"
            lines.append(
                dir_emoji + " <b>" + coin + "</b>: объём ×" + "{:.1f}".format(ratio) + " к среднему\n"
                + "   Текущий: $" + "{:,.0f}".format(cur_usd) + " | Средний: $" + "{:,.0f}".format(avg_usd)
            )
        line = "------------------------------"
        text = (
            "&#128202; <b>ВСПЛЕСК ОБЪЁМА (1ч) [ТЕСТ]</b>\n"
            + line + "\n"
            + "\n".join(lines) + "\n"
            + line + "\n"
            + "<i>Usoltsev Signals</i>"
        )
        send_telegram(text, THREAD_VOLUME)
        sent = True

    return json.dumps({
        "sent_to_telegram": sent,
        "hits_count": len(hits),
        "thread_id_used": THREAD_VOLUME,
        "per_coin": debug_rows
    }, indent=2, ensure_ascii=False), 200, {"Content-Type": "application/json; charset=utf-8"}

@app.route("/test_claude_key")
def test_claude_key():
    """Честная диагностика ключа Anthropic: минимальный вызов API (без свечей, без
    промпта анализа) — только чтобы проверить, что ключ авторизуется и модель отвечает."""
    text, error = call_claude_opus(
        "Ты — тестовый ассистент.",
        "Ответь одним словом: 'работает'.",
        max_tokens=20
    )
    return json.dumps({
        "key_is_set": bool(ANTHROPIC_API_KEY),
        "model": CLAUDE_MODEL,
        "success": error is None,
        "response_text": text,
        "error": error
    }, indent=2, ensure_ascii=False), 200, {"Content-Type": "application/json; charset=utf-8"}

@app.route("/test_btc_analysis")
def test_btc_analysis():
    """Разовый принудительный запуск полного BTC-анализа с реальной отправкой в
    Telegram (тема General) — не дожидаясь 8 часов. Полезно для проверки всего
    пайплайна целиком: свечи -> метрики -> промпт -> Opus -> отправка."""
    text, error = run_btc_analysis(send=True)
    return json.dumps({
        "sent_to_telegram": error is None,
        "error": error,
        "preview": text[:800] if text else None
    }, indent=2, ensure_ascii=False), 200, {"Content-Type": "application/json; charset=utf-8"}

@app.route("/test_trade_signal")
def test_trade_signal():
    """Разовый принудительный запуск торгового модуля (LONG/SHORT/нет сигнала)
    с реальной отправкой в Telegram — не дожидаясь 6:00 или 18:00."""
    text, error = run_trade_signal(send=True)
    return json.dumps({
        "sent_to_telegram": error is None,
        "error": error,
        "preview": text[:800] if text else None
    }, indent=2, ensure_ascii=False), 200, {"Content-Type": "application/json; charset=utf-8"}

@app.route("/send_ticker_list")
def send_ticker_list():
    """Отправляет в тему Ask Analysis столбиком список доступных для запроса
    тикеров: крипто (WHALE_COINS — те же 24 монеты, что и в whale/volume модулях,
    в теории можно запросить и любой другой перпетуал Bybit, но эти проверенно
    резолвятся) отдельной группой, акции xStocks отдельной группой."""
    lines = []
    lines.append("&#128203; <b>Доступные тикеры для Ask Analysis</b>")
    lines.append("------------------------------")
    lines.append("&#129377; <b>Крипто (Bybit перпетуалы):</b>")
    for coin in WHALE_COINS:
        lines.append("• " + coin)
    lines.append("")
    lines.append("&#128200; <b>Акции (xStocks, спот):</b>")
    for stock in sorted(XSTOCKS_TICKERS):
        lines.append("• " + stock)
    lines.append("------------------------------")
    lines.append("<i>Можно запросить и другие крипто-тикеры, которых нет в списке "
                  "выше — если они торгуются на Bybit как перпетуал, бот попробует "
                  "их найти автоматически. Формат: ТИКЕР ТАЙМФРЕЙМ, например "
                  "'fartcoinusdt 4h' или 'aapl 1d'.</i>")
    text = "\n".join(lines)
    send_telegram(text, THREAD_ASK_ANALYSIS)
    return "Список отправлен в тему Ask Analysis", 200

@app.route("/test_ask_analysis")
def test_ask_analysis():
    """Разовый ручной тест модуля Ask Analysis через query-параметр ?q=,
    например /test_ask_analysis?q=fartcoinusdt 4h — обходит cooldown и шлёт
    результат прямо в тему Ask Analysis."""
    q = request.args.get("q", "fartcoinusdt 4h")
    global _ask_analysis_last_ts
    with _ask_analysis_lock:
        _ask_analysis_last_ts = 0   # обход cooldown для ручного теста
    ok, error = run_ask_analysis(q)
    return json.dumps({"query": q, "ok": ok, "error": error}, indent=2, ensure_ascii=False), 200, {"Content-Type": "application/json; charset=utf-8"}

@app.route("/test_flush_whale")
def test_flush_whale():
    """Принудительно сбросить накопленный буфер прямо сейчас, не дожидаясь запланированного времени."""
    n = len(whale_alert_buffer)
    flush_whale_alerts()
    return "Сброшено записей: " + str(n), 200

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
# Дедупликация апдейтов Telegram: если webhook не успевает ответить достаточно
# быстро (например, пока в фоне рисуется график), Telegram может повторно
# прислать ТОТ ЖЕ update_id — без защиты это приводило к цепочке дублирующихся
# ответов (в т.ч. дублей "слишком частые запросы"). Небольшой set с капом —
# этого достаточно, повторы приходят в течение секунд-минут, не часов.
_seen_update_ids = set()
_seen_update_ids_lock = threading.Lock()

@app.route("/telegram_updates", methods=["POST"])
def telegram_updates():
    try:
        update = request.json or {}
    except:
        update = {}

    update_id = update.get("update_id")
    if update_id is not None:
        with _seen_update_ids_lock:
            if update_id in _seen_update_ids:
                return "OK", 200   # уже обработан — тихо игнорируем повтор
            _seen_update_ids.add(update_id)
            if len(_seen_update_ids) > 2000:
                _seen_update_ids.clear()

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
    elif thread_id == THREAD_ASK_ANALYSIS and not text.startswith("/"):
        # Любое сообщение (не команда) в теме Ask Analysis трактуется как запрос
        # на анализ. Запускаем в отдельном потоке, чтобы не блокировать webhook
        # Telegram (рисование графика + вызов API может занять несколько секунд).
        def _run():
            ok, err = run_ask_analysis(text)
            if not ok and err:
                send_telegram("&#9888; " + err, THREAD_ASK_ANALYSIS)
        threading.Thread(target=_run, daemon=True).start()

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

def flush_whale_alerts():
    """Раз в 30 минут: группируем накопленные записи по монете и считаем net-баланс
    (накопление - продажа) в один компактный вывод на строку — вариант А форматирования.
    transfer (кошелёк-кошелёк) не входит ни в net, ни в счётчики сделок — слишком шумный
    и не несёт сигнала покупки/продажи, но продолжает учитываться в coin_total,
    чтобы сортировка монет по общему объёму не менялась резко.
    Без привязки к конкретной бирже — данные идут из сети Ethereum по контракту токена,
    не по конкретному известному адресу."""
    with whale_alert_lock:
        if not whale_alert_buffer:
            return
        entries = whale_alert_buffer.copy()
        whale_alert_buffer.clear()

    # coin -> {"buy": [value,...], "sell": [...], "transfer": [...], "chain": "ETH"}
    grouped = {}
    for e in entries:
        coin = e.get("coin", "?")
        cat  = e.get("category", "transfer")
        val  = e.get("value", 0) or 0
        chain = e.get("chain", "ETH")
        g = grouped.setdefault(coin, {"buy": [], "sell": [], "transfer": [], "chain": chain})
        if cat not in ("buy", "sell", "transfer"):
            cat = "transfer"
        g[cat].append(val)

    # сортировка монет по суммарному объёму (buy+sell+transfer), по убыванию
    def coin_total(coin):
        g = grouped[coin]
        return sum(g["buy"]) + sum(g["sell"]) + sum(g["transfer"])

    coins_sorted = sorted(grouped.keys(), key=coin_total, reverse=True)

    # Вариант А: одна компактная строка на монету с net-балансом (накопление - продажа).
    # transfer (кошелёк-кошелёк) по-прежнему не входит в net и в счётчики — слишком шумный
    # и не несёт сигнала покупки/продажи, но участвует в coin_total для сортировки монет.
    # Эмодзи в начале строки — какая сторона перевесила, в скобках — счётчик сделок
    # с каждой стороны (buyN🟢/sellN🔴).
    lines = []
    for coin in coins_sorted:
        g = grouped[coin]
        buy_vals  = g["buy"]
        sell_vals = g["sell"]
        if not buy_vals and not sell_vals:
            continue   # у монеты были только transfer-события — в сводке не показываем вообще

        buy_sum  = sum(buy_vals)
        sell_sum = sum(sell_vals)
        buy_cnt  = len(buy_vals)
        sell_cnt = len(sell_vals)
        net = buy_sum - sell_sum

        if net > 0:
            net_emoji = "&#128994;"   # 🟢 накопление перевесило
        elif net < 0:
            net_emoji = "&#128308;"   # 🔴 продажа перевесила
        else:
            net_emoji = "&#128993;"   # 🟡 равновесие

        parts = []
        if buy_cnt:
            parts.append(str(buy_cnt) + "&#128994;")
        if sell_cnt:
            parts.append(str(sell_cnt) + "&#128308;")
        counts_str = " / ".join(parts)

        lines.append(
            net_emoji + " <b>" + coin + "</b>: " + "{:+,.0f}".format(net) + "$"
            + "  (" + counts_str + ")"
        )

    line = "------------------------------"
    total_events = sum(1 for e in entries if e.get("category") in ("buy", "sell"))
    if total_events == 0:
        return   # все события за это окно оказались transfer — сводку не шлём вообще
    event_word = "событие" if total_events == 1 else ("событий" if total_events == 1 or total_events >= 5 or (11 <= total_events % 100 <= 14) else "события")
    text = (
        "&#128011; <b>КИТОВАЯ СВОДКА</b> (" + str(total_events) + " " + event_word + ")\n"
        + line + "\n"
        + "\n".join(lines) + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals</i>"
    )
    send_telegram(text, THREAD_WHALE)

scheduler = BackgroundScheduler()
scheduler.add_job(daily_report, "cron", hour=4,  minute=0)
scheduler.add_job(daily_report, "cron", hour=10, minute=0)
scheduler.add_job(daily_report, "cron", hour=14, minute=0)
scheduler.add_job(flush_whale_alerts, "interval", hours=4)
scheduler.start()

# ═══════════════════════════════════════════════
# ОНЧЕЙН КИТЫ (спотовые закупки, Ethereum + Solana)
# ═══════════════════════════════════════════════
ETHERSCAN_API_KEY = os.environ.get("ETHERSCAN_API_KEY")
SOLSCAN_API_KEY   = os.environ.get("SOLSCAN_API_KEY")
# Онлайн-киты идут в ту же тему, что и биржевые киты — THREAD_WHALE (591), уже объявлена выше

ONCHAIN_USD_THRESHOLD        = 100000
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
    """Раз в час: топ-N монет по объёму с CoinGecko + их адреса контрактов на Ethereum/Solana.
    Фильтруем мусорные/накрученные токены (нулевая или отсутствующая капитализация,
    нулевое циркулирующее предложение — классика wash-trading схем типа MPRA), а также
    стейблкоины — их переводы это расчёты/ребалансировка бирж, не сигнал накопления актива.
    Стейблы отсекаем ДВУМЯ способами: по официальной категории CoinGecko (надёжнее, ловит
    любые стейблы независимо от тикера) + запасной жёстко заданный список на случай сбоя категории."""
    MIN_MARKET_CAP = 5_000_000   # $5M — ниже считаем токен подозрительным для трекинга
    STABLECOINS = {
        "USDT", "USDC", "USDS", "USDG", "USD1", "DAI", "TUSD", "USDP", "FDUSD",
        "PYUSD", "USDE", "GUSD", "FRAX", "LUSD", "USDD", "CRVUSD", "SUSD", "USDK"
    }
    stablecoin_ids = set()
    try:
        cat_url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&category=stablecoins&per_page=250&page=1"
        cat_coins = requests.get(cat_url, timeout=15).json()
        if isinstance(cat_coins, list):
            stablecoin_ids = {c.get("id") for c in cat_coins if c.get("id")}
    except:
        pass

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
            market_cap = coin.get("market_cap", 0) or 0
            circ_supply = coin.get("circulating_supply", 0) or 0
            symbol = (coin.get("symbol") or "").upper()

            # отсекаем мусорные/накрученные токены: нет реальной капитализации
            # или нулевое циркулирующее предложение (типичный признак фейкового объёма)
            if market_cap < MIN_MARKET_CAP or circ_supply <= 0:
                continue
            # отсекаем стейблкоины — по категории CoinGecko (основной способ) + список тикеров (запасной)
            if cid in stablecoin_ids or symbol in STABLECOINS:
                continue

            plats = plat_by_id.get(cid, {})
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
    """Категория определяется по известным адресам бирж, если они есть (buy/sell),
    иначе это перевод между обычными кошельками (transfer). Данные всегда берутся
    из сети (Ethereum), привязка к конкретной бирже — не обязательна для категории."""
    from_tag = KNOWN_EXCHANGE_ETH.get(from_addr, KNOWN_EXCHANGE_SOL.get(from_addr, ""))
    to_tag   = KNOWN_EXCHANGE_ETH.get(to_addr,   KNOWN_EXCHANGE_SOL.get(to_addr, ""))

    if to_tag:
        category = "sell"       # уход на биржу — возможная продажа
    elif from_tag:
        category = "buy"        # приход с биржи на кошелёк — возможное накопление
    else:
        category = "transfer"   # кошелёк ↔ кошелёк

    with whale_alert_lock:
        whale_alert_buffer.append({"coin": symbol, "category": category, "value": usd_value, "chain": chain_name})

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

# ═══════════════════════════════════════════════
# ВСПЛЕСКИ ОБЪЁМА (наши тикеры, проверка раз в 2 часа, порог x2.5, гистерезис x1.2)
# ═══════════════════════════════════════════════
# Логика: раз в VOL_POLL_SECONDS берём последние часовые свечи по каждой монете из
# WHALE_COINS, считаем средний объём предыдущих 20 ЗАКРЫТЫХ часовых свечей и
# сравниваем с объёмом последней закрытой свечи. Если отношение >= VOL_ALERT_RATIO —
# кандидат на алерт. Гистерезис против дребезга: после срабатывания монета "молчит",
# пока отношение не опустится ниже VOL_RESET_RATIO (реально успокоилось), и только
# тогда снова может сработать. Все монеты, всплывшие за один проход, идут одной сводкой.
VOL_ALERT_RATIO   = 2.5      # было 1.5 — поднято по итогам ~2 недель наблюдений
VOL_RESET_RATIO   = 1.2
VOL_AVG_BARS      = 20     # сколько предыдущих закрытых часовых свечей берём для среднего
VOL_POLL_SECONDS  = 7200   # было 900 (15 минут) — теперь раз в 2 часа, для общего обзора

# coin -> "armed" (может сработать) | "cooldown" (ждём остывания ниже VOL_RESET_RATIO)
volume_alert_state = {}

def get_hourly_klines(symbol, limit=21):
    """limit по умолчанию не используется явно — вызывающий код передаёт VOL_AVG_BARS+2:
    1 текущая (возможно незакрытая) + 1 последняя закрытая + 20 баров для среднего.
    Bybit kline интервал '60' = 1ч."""
    try:
        url = ("https://api.bybit.com/v5/market/kline?category=linear&symbol=" + symbol
               + "&interval=60&limit=" + str(limit))
        r = requests.get(url, timeout=10).json()
        rows = r.get("result", {}).get("list", [])
        # Bybit отдаёт от новых к старым, каждая строка: [start, open, high, low, close, volume, turnover]
        return rows
    except:
        return []

def volume_alert_loop():
    # один раз находим реальные тикеры Bybit для монет (переиспользуем тот же
    # резолвер и кэш, что и биржевые whale-алерты — тикеры те же самые)
    for coin in WHALE_COINS:
        if coin not in whale_symbol_cache:
            whale_symbol_cache[coin] = resolve_whale_symbol(coin)
            time.sleep(0.3)

    while True:
        try:
            hits = []   # список (coin, ratio, cur_vol_usd, avg_vol_usd, is_green)
            for coin in WHALE_COINS:
                sym = whale_symbol_cache.get(coin)
                if not sym:
                    continue
                rows = get_hourly_klines(sym, VOL_AVG_BARS + 2)
                if len(rows) < VOL_AVG_BARS + 2:
                    continue

                # rows[0] — последняя свеча. Если она ещё не закрылась (текущий час в процессе),
                # Bybit всё равно её отдаёт первой — пропускаем самую свежую и берём следующую
                # как "последнюю закрытую", а под неё — VOL_AVG_BARS свечей для среднего.
                last_closed = rows[1]
                avg_rows = rows[2:2 + VOL_AVG_BARS]
                if len(avg_rows) < VOL_AVG_BARS:
                    continue

                try:
                    cur_vol = float(last_closed[5])
                    cur_close = float(last_closed[4])
                    cur_open = float(last_closed[1])
                    avg_vol = sum(float(r[5]) for r in avg_rows) / len(avg_rows)
                except:
                    continue

                if avg_vol <= 0:
                    continue

                ratio = cur_vol / avg_vol
                state = volume_alert_state.get(coin, "armed")

                if state == "armed" and ratio >= VOL_ALERT_RATIO:
                    cur_vol_usd = cur_vol * cur_close
                    avg_vol_usd = avg_vol * cur_close
                    is_green = cur_close >= cur_open
                    hits.append((coin, ratio, cur_vol_usd, avg_vol_usd, is_green))
                    volume_alert_state[coin] = "cooldown"
                elif state == "cooldown" and ratio < VOL_RESET_RATIO:
                    volume_alert_state[coin] = "armed"

            if hits:
                hits.sort(key=lambda h: h[1], reverse=True)
                lines = []
                for coin, ratio, cur_usd, avg_usd, is_green in hits:
                    dir_emoji = "&#128994;" if is_green else "&#128308;"
                    lines.append(
                        dir_emoji + " <b>" + coin + "</b>: объём ×" + "{:.1f}".format(ratio) + " к среднему\n"
                        + "   Текущий: $" + "{:,.0f}".format(cur_usd) + " | Средний: $" + "{:,.0f}".format(avg_usd)
                    )
                line = "------------------------------"
                text = (
                    "&#128202; <b>ВСПЛЕСК ОБЪЁМА (1ч)</b>\n"
                    + line + "\n"
                    + "\n".join(lines) + "\n"
                    + line + "\n"
                    + "<i>Usoltsev Signals</i>"
                )
                send_telegram(text, THREAD_VOLUME)
        except:
            pass
        time.sleep(VOL_POLL_SECONDS)

# ═══════════════════════════════════════════════
# АНАЛИЗ BTC (Claude Opus 5, раз в 8 часов, тема General)
# ═══════════════════════════════════════════════
# Разделение труда: Python точно считает EMA50/EMA200/RSI/объём/уровни по формулам
# на реальных свечах Bybit — эти цифры гарантированно верны, никакой арифметики
# на откуп модели. Opus 5 получает уже готовые цифры + сырые свечи (для контекста
# истории) + свежие новости (переиспользуем те же RSS, что и daily_report) + наши
# собственные данные (китовая активность, объёмный сканер) и пишет ТОЛЬКО текст:
# сценарии с вероятностями, риски, триггеры, резюме дня — по образцу, который
# прислал пользователь (скриншот канала "Лев и Киса").
CLAUDE_MODEL   = "claude-opus-5"
BTC_ANALYSIS_INTERVAL_HOURS = 8

def get_btc_klines(interval, limit):
    """Свечи BTCUSDT с Bybit. interval: '240' = 4ч, 'D' = дневные.
    Возвращает список от старых к новым (разворачиваем, Bybit отдаёт от новых к старым)."""
    try:
        url = ("https://api.bybit.com/v5/market/kline?category=linear&symbol=BTCUSDT"
               + "&interval=" + str(interval) + "&limit=" + str(limit))
        r = requests.get(url, timeout=15).json()
        rows = r.get("result", {}).get("list", [])
        rows = list(reversed(rows))  # от старых к новым
        return rows
    except:
        return []

def calc_ema(closes, period):
    """EMA по списку цен закрытия (от старых к новым). Возвращает значение на последнем баре."""
    if len(closes) < period:
        return None
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period   # старт — простое среднее первых period значений
    for price in closes[period:]:
        ema = price * k + ema * (1 - k)
    return ema

def calc_rsi(closes, period=14):
    """RSI по стандартной формуле Wilder's smoothing на списке цен закрытия."""
    if len(closes) < period + 1:
        return None
    gains = []
    losses = []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))

def fmt_price(value):
    """Форматирует цену в ОБЫЧНОЙ десятичной записи (никогда не в экспоненциальной,
    как это делал прежний "{:,.4g}" на больших числах вроде цены BTC — там 4
    значащие цифры на 5-значном числе переключали Python на запись вида
    "6.313e+04"). Число знаков после запятой подбирается под порядок величины,
    чтобы одинаково хорошо выглядели и BTC (~$63,000), и LINK (~$8.7), и мелкие
    дробные тикеры вроде мемкоинов (~$0.0001423)."""
    v = abs(value)
    if v >= 1000:
        return "{:,.0f}".format(value)
    elif v >= 1:
        return "{:,.2f}".format(value)
    elif v >= 0.01:
        return "{:,.4f}".format(value)
    else:
        s = "{:,.8f}".format(value).rstrip("0").rstrip(".")
        return s

def find_support_resistance(rows, lookback=60):
    """Простой поиск ближайших уровней: локальные хаи/лоу за последние lookback баров
    (пивот = экстремум среди 2 соседей с каждой стороны), берём ближайший снизу и сверху
    от текущей цены."""
    if len(rows) < lookback:
        lookback = len(rows)
    recent = rows[-lookback:]
    highs = [float(r[2]) for r in recent]
    lows  = [float(r[3]) for r in recent]
    cur_price = float(rows[-1][4])

    pivot_highs = []
    pivot_lows = []
    for i in range(2, len(recent) - 2):
        if highs[i] == max(highs[i-2:i+3]):
            pivot_highs.append(highs[i])
        if lows[i] == min(lows[i-2:i+3]):
            pivot_lows.append(lows[i])

    resistance = min([h for h in pivot_highs if h > cur_price], default=None)
    support = max([l for l in pivot_lows if l < cur_price], default=None)
    return support, resistance, cur_price

def build_btc_metrics():
    """Считает все числовые метрики по BTC: EMA/RSI на 4ч, объём к среднему,
    ближайшие уровни поддержки/сопротивления. Возвращает словарь с готовыми цифрами
    и сырыми свечами (для передачи модели как контекст истории)."""
    rows_4h  = get_btc_klines("240", 200)   # ~33 дня по 4ч барам
    rows_1d  = get_btc_klines("D", 90)      # 90 дней дневных

    if len(rows_4h) < 60 or len(rows_1d) < 30:
        return None

    closes_4h = [float(r[4]) for r in rows_4h]
    vols_4h   = [float(r[5]) for r in rows_4h]

    ema50  = calc_ema(closes_4h, 50)
    ema200 = calc_ema(closes_4h, 200) if len(closes_4h) >= 200 else calc_ema(closes_4h, min(200, len(closes_4h) - 1))
    rsi14  = calc_rsi(closes_4h, 14)

    cur_vol = vols_4h[-1]
    avg_vol_4h = sum(vols_4h[-21:-1]) / 20 if len(vols_4h) >= 21 else sum(vols_4h) / len(vols_4h)
    vol_ratio = cur_vol / avg_vol_4h if avg_vol_4h > 0 else 0

    support, resistance, cur_price = find_support_resistance(rows_4h, lookback=60)

    change_24h_4h_bars = 6  # 6 баров по 4ч = 24 часа
    price_24h_ago = closes_4h[-1 - change_24h_4h_bars] if len(closes_4h) > change_24h_4h_bars else closes_4h[0]
    change_24h_pct = (cur_price - price_24h_ago) / price_24h_ago * 100 if price_24h_ago else 0

    return {
        "price": cur_price,
        "change_24h_pct": change_24h_pct,
        "ema50": ema50,
        "ema200": ema200,
        "rsi14": rsi14,
        "vol_ratio": vol_ratio,
        "support": support,
        "resistance": resistance,
        "rows_4h_tail": rows_4h[-42:],   # последние ~7 дней 4ч баров для контекста модели
        "rows_1d_tail": rows_1d[-30:],   # последний месяц дневных баров
    }

def get_crypto_news_headlines():
    """Переиспользует те же RSS-источники, что и daily_report, но берёт чуть больше
    заголовков специально для BTC-анализа (до 6 вместо 3)."""
    sources = [
        "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "https://cointelegraph.com/rss",
        "https://decrypt.co/feed",
    ]
    headlines = []
    for url in sources:
        if len(headlines) >= 6:
            break
        try:
            headers = {"User-Agent": "Mozilla/5.0"}
            r = requests.get(url, headers=headers, timeout=6)
            root = ET.fromstring(r.content)
            items = root.findall(".//item")
            for item in items[:3]:
                title = item.find("title")
                if title is not None and title.text:
                    text = title.text.strip()
                    if len(text) > 120:
                        text = text[:120] + "..."
                    headlines.append(text)
                    if len(headlines) >= 6:
                        break
        except:
            continue
    return headlines

def call_claude_opus(system_prompt, user_prompt, max_tokens=1500):
    """Один вызов Anthropic API. Возвращает (text, error) — error=None при успехе,
    иначе строка с описанием проблемы (для диагностики через /test_claude_key)."""
    if not ANTHROPIC_API_KEY:
        return None, "ANTHROPIC_API_KEY не задан в переменных окружения"
    try:
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": CLAUDE_MODEL,
            "max_tokens": max_tokens,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        if r.status_code != 200:
            return None, "HTTP " + str(r.status_code) + ": " + r.text[:500]
        data = r.json()
        blocks = data.get("content", [])
        text_parts = [b.get("text", "") for b in blocks if b.get("type") == "text"]
        text = "\n".join(text_parts).strip()
        if not text:
            return None, "Пустой ответ от API: " + json.dumps(data)[:500]
        return text, None
    except requests.exceptions.Timeout:
        return None, "TIMEOUT — запрос к Anthropic API не получил ответ за 60 секунд"
    except Exception as e:
        return None, "OTHER: " + type(e).__name__ + ": " + str(e)

def build_btc_analysis_prompt(metrics, news, whale_summary, volume_summary):
    """Собирает system + user промпт для Opus на основе готовых Python-расчётов
    и сырого контекста (свечи/новости/наши собственные данные)."""
    system_prompt = (
        "Ты — опытный крипто-аналитик, пишущий короткие структурированные обзоры BTC "
        "для приватного Telegram-канала трейдера. Стиль: по делу, без воды, конкретные "
        "уровни и цифры. Структура ответа СТРОГО такая (используй HTML-теги Telegram: "
        "<b>жирный</b>, обычный перенос строки — НЕ используй markdown ** или #):\n\n"
        "1) Заголовок одной фразой, отражающий суть текущей картины.\n"
        "2) Блок 'Где мы:' — 2-4 предложения: текущая цена, тренд по EMA, RSI, объём, "
        "куда давит рынок.\n"
        "3) 2-3 сценария (например 'Пробой вниз', 'Боковик', 'Отскок вверх'), каждый с "
        "оценкой вероятности в % (сумма всех сценариев = 100%), кратким описанием, "
        "конкретным триггером (что должно произойти технически) и целевым уровнем.\n"
        "4) Блок 'Суть дня:' — 2-3 предложения итогового вывода и на что смотреть.\n\n"
        "Проценты вероятности — твоя экспертная оценка на основе предоставленных цифр, "
        "истории свечей и новостного фона. Не выдумывай технические цифры (EMA, RSI, "
        "уровни) — используй ТОЛЬКО те, что даны тебе явно ниже, они посчитаны точно "
        "по формулам. Свечи и новости — это твой контекст для оценки вероятностей и "
        "рассуждения, не источник для новых числовых расчётов."
    )

    lines = []
    lines.append("ТЕКУЩИЕ ДАННЫЕ ПО BTC (посчитаны точно, используй как есть):")
    lines.append("Цена: $" + "{:,.0f}".format(metrics["price"]))
    lines.append("Изменение за 24ч: " + "{:+.2f}".format(metrics["change_24h_pct"]) + "%")
    if metrics["ema50"]:
        lines.append("EMA50 (4ч): $" + "{:,.0f}".format(metrics["ema50"]))
    if metrics["ema200"]:
        lines.append("EMA200 (4ч): $" + "{:,.0f}".format(metrics["ema200"]))
    if metrics["rsi14"] is not None:
        lines.append("RSI(14) на 4ч: " + "{:.1f}".format(metrics["rsi14"]))
    lines.append("Текущий объём (4ч бар) к среднему за 20 баров: ×" + "{:.2f}".format(metrics["vol_ratio"]))
    if metrics["support"]:
        lines.append("Ближайшая поддержка: $" + "{:,.0f}".format(metrics["support"]))
    if metrics["resistance"]:
        lines.append("Ближайшее сопротивление: $" + "{:,.0f}".format(metrics["resistance"]))

    lines.append("")
    lines.append("СВЕЖИЕ НОВОСТИ (для оценки фона, не для технических расчётов):")
    if news:
        for h in news:
            lines.append("- " + h)
    else:
        lines.append("(новости недоступны в этот раз)")

    if whale_summary:
        lines.append("")
        lines.append("НАША КИТОВАЯ АКТИВНОСТЬ (последние данные из системы):")
        lines.append(whale_summary)

    if volume_summary:
        lines.append("")
        lines.append("НАШИ ДАННЫЕ ПО ВСПЛЕСКАМ ОБЪЁМА (последние данные из системы):")
        lines.append(volume_summary)

    lines.append("")
    lines.append("ИСТОРИЯ СВЕЧЕЙ ДЛЯ КОНТЕКСТА (4ч, последние ~7 дней, "
                  "формат [время, open, high, low, close, volume]):")
    for row in metrics["rows_4h_tail"]:
        lines.append(str(row[:6]))

    lines.append("")
    lines.append("ИСТОРИЯ СВЕЧЕЙ ДЛЯ КОНТЕКСТА (дневные, последний месяц, "
                  "формат [время, open, high, low, close, volume]):")
    for row in metrics["rows_1d_tail"]:
        lines.append(str(row[:6]))

    user_prompt = "\n".join(lines)
    return system_prompt, user_prompt

def get_last_whale_summary_text(coin="BTC"):
    """Короткая сводка последних китовых данных из буфера для указанной монеты —
    не отправка, просто текстовый снимок для контекста промпта анализа."""
    with whale_alert_lock:
        entries = list(whale_alert_buffer)
    if not entries:
        return None
    coin_entries = [e for e in entries if e.get("coin") == coin and e.get("category") in ("buy", "sell")]
    if not coin_entries:
        return None
    buy_sum = sum(e["value"] for e in coin_entries if e["category"] == "buy")
    sell_sum = sum(e["value"] for e in coin_entries if e["category"] == "sell")
    return coin + " накопление: $" + "{:,.0f}".format(buy_sum) + " | " + coin + " продажа: $" + "{:,.0f}".format(sell_sum)

def get_last_volume_summary_text(coin="BTC"):
    """Текущий коэффициент объёма указанной монеты (из того же расчёта, что и
    volume_alert_loop) — короткий снимок для контекста промпта."""
    sym = whale_symbol_cache.get(coin)
    if not sym:
        return None
    rows = get_hourly_klines(sym, VOL_AVG_BARS + 2)
    if len(rows) < VOL_AVG_BARS + 2:
        return None
    last_closed = rows[1]
    avg_rows = rows[2:2 + VOL_AVG_BARS]
    try:
        cur_vol = float(last_closed[5])
        avg_vol = sum(float(r[5]) for r in avg_rows) / len(avg_rows)
        ratio = cur_vol / avg_vol if avg_vol > 0 else 0
        return "Часовой объём " + coin + " сейчас: ×" + "{:.2f}".format(ratio) + " к среднему"
    except:
        return None

def run_btc_analysis(send=True):
    """Полный проход: считает метрики, собирает промпт, вызывает Opus, при send=True
    отправляет в THREAD_GENERAL. Возвращает (text, error) для диагностики."""
    metrics = build_btc_metrics()
    if metrics is None:
        return None, "Не удалось получить достаточно свечей BTC с Bybit"

    news = get_crypto_news_headlines()
    whale_summary = get_last_whale_summary_text()
    volume_summary = get_last_volume_summary_text()

    system_prompt, user_prompt = build_btc_analysis_prompt(metrics, news, whale_summary, volume_summary)
    text, error = call_claude_opus(system_prompt, user_prompt)
    if error:
        return None, error

    line = "------------------------------"
    full_text = (
        "&#8383; <b>АНАЛИЗ BTC</b>\n"
        + datetime.now().strftime("%d.%m.%Y  %H:%M") + " (UTC+5)\n"
        + line + "\n"
        + text + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals · " + CLAUDE_MODEL + "</i>"
    )

    if send:
        send_telegram(full_text, THREAD_GENERAL)

    return full_text, None

def btc_analysis_loop():
    """Фоновый поток: раз в BTC_ANALYSIS_INTERVAL_HOURS часов запускает полный анализ
    и шлёт в тему General. Ошибки логируются в per-iteration try/except, чтобы одна
    неудачная попытка не убила поток навсегда."""
    while True:
        try:
            run_btc_analysis(send=True)
        except:
            pass
        time.sleep(BTC_ANALYSIS_INTERVAL_HOURS * 3600)

# ═══════════════════════════════════════════════
# ТОРГОВЫЙ СИГНАЛ (Claude Opus 5, 4 раза в день, тема General)
# ═══════════════════════════════════════════════
# Мультитикерный модуль — BTC, ETH, LINK в одном сообщении. Логика та же:
# Python считает уровни ТОЧНО по формуле (вход у ближайшего значимого уровня,
# стоп за уровнем с запасом в ATR, тейк — следующий уровень или R:R 1:2 как
# запасной вариант), Opus получает готовые цифры + весь контекст (свечи, новости,
# наши китовые/объёмные данные) и либо даёт сигнал с обоснованием, либо явно
# пишет "чёткого сигнала нет" — если её собственная уверенность не высокая
# или контекст противоречивый. Модель НЕ придумывает цифры уровней сама.
# Использует те же универсальные функции метрик/свечей, что и Ask Analysis
# (build_universal_metrics, get_klines_universal, resolve_symbol_and_category),
# чтобы не дублировать BTC-специфичную логику под каждый новый тикер.
TRADE_SIGNAL_HOURS_UTC5 = [7, 14, 18, 22]   # время отправки, UTC+5 (соответствует
                                              # 5:00 / 12:00 / 16:00 / 20:00 МСК)
TRADE_SIGNAL_TICKERS = ["BTC", "ETH", "LINK"]

def calc_atr(rows, period=14):
    """ATR по списку свечей [start, open, high, low, close, volume, turnover] —
    True Range усредняется простым SMA за period баров (последнее значение)."""
    if len(rows) < period + 1:
        return None
    trs = []
    for i in range(1, len(rows)):
        high = float(rows[i][2])
        low  = float(rows[i][3])
        prev_close = float(rows[i - 1][4])
        tr = max(high - low, abs(high - prev_close), abs(low - prev_close))
        trs.append(tr)
    if len(trs) < period:
        return None
    return sum(trs[-period:]) / period

def find_next_level(rows, ref_price, direction, lookback=120):
    """Ищет следующий пивотный уровень дальше по цепочке от ref_price в направлении
    сделки (для тейка). direction: 'up' — следующее сопротивление выше ref_price,
    'down' — следующая поддержка ниже ref_price."""
    if len(rows) < lookback:
        lookback = len(rows)
    recent = rows[-lookback:]
    highs = [float(r[2]) for r in recent]
    lows  = [float(r[3]) for r in recent]

    pivot_highs = []
    pivot_lows = []
    for i in range(2, len(recent) - 2):
        if highs[i] == max(highs[i-2:i+3]):
            pivot_highs.append(highs[i])
        if lows[i] == min(lows[i-2:i+3]):
            pivot_lows.append(lows[i])

    if direction == "up":
        candidates = sorted([h for h in pivot_highs if h > ref_price])
    else:
        candidates = sorted([l for l in pivot_lows if l < ref_price], reverse=True)
    return candidates[0] if candidates else None

def build_trade_levels(metrics, rows_4h):
    """Считает конкретные торговые уровни ТОЧНО по формуле для обоих направлений
    (long и short) — Opus далее выберет, какое (если любое) обосновано, но сами
    числа фиксированы кодом, не моделью."""
    atr = calc_atr(rows_4h, 14)
    if atr is None or not metrics.get("support") or not metrics.get("resistance"):
        return None

    support = metrics["support"]
    resistance = metrics["resistance"]
    cur_price = metrics["price"]

    # LONG: вход в зоне поддержки (небольшой буфер внутрь диапазона), стоп ниже
    # поддержки на 0.5 ATR, тейк — следующий уровень сопротивления дальше по цепочке,
    # либо R:R 1:2 как запасной вариант, если следующего уровня не нашлось.
    long_entry = support + atr * 0.1
    long_stop  = support - atr * 0.5
    long_risk  = long_entry - long_stop
    long_next_level = find_next_level(rows_4h, resistance, "up")
    long_take = long_next_level if long_next_level else long_entry + long_risk * 2
    long_rr = (long_take - long_entry) / long_risk if long_risk > 0 else None

    # SHORT: зеркально у сопротивления
    short_entry = resistance - atr * 0.1
    short_stop  = resistance + atr * 0.5
    short_risk  = short_stop - short_entry
    short_next_level = find_next_level(rows_4h, support, "down")
    short_take = short_next_level if short_next_level else short_entry - short_risk * 2
    short_rr = (short_entry - short_take) / short_risk if short_risk > 0 else None

    return {
        "atr": atr,
        "long":  {"entry": long_entry,  "stop": long_stop,  "take": long_take,  "rr": long_rr},
        "short": {"entry": short_entry, "stop": short_stop, "take": short_take, "rr": short_rr},
        "cur_price": cur_price,
    }

def build_trade_signal_prompt(ticker_label, metrics, levels, rows_4h_tail, news, whale_summary, volume_summary):
    """Промпт для решения long/short/нет сигнала по указанному тикеру. Числа даны
    Opus только как контекст для принятия решения — сама карточка с цифрами
    (вход/стоп/тейк/R:R) собирается кодом Python отдельно из levels, НЕ из ответа
    модели. Поэтому от Opus нужен ТОЛЬКО чистый вердикт + короткое текстовое
    обоснование БЕЗ повторения цифр — цифры в ответе не нужны и могут разъехаться
    с реальными levels."""
    system_prompt = (
        "Ты — крипто-трейдер, принимающий решение о конкретной сделке по " + ticker_label + " для "
        "приватного Telegram-канала. У тебя есть ДВА готовых набора уровней (long и "
        "short, посчитаны точно по формуле) — они показаны тебе только для контекста "
        "принятия решения, HE вставляй их числа в свой ответ, они будут добавлены "
        "отдельно кодом. Выбери один из трёх вариантов:\n"
        "1) LONG — если контекст ясно говорит в пользу покупки у поддержки.\n"
        "2) SHORT — если контекст ясно говорит в пользу продажи у сопротивления.\n"
        "3) НЕТ СИГНАЛА — если ситуация неоднозначная, противоречивая, или ни один "
        "сценарий не выглядит явно предпочтительным. НЕ бойся выбрать этот вариант — "
        "лучше промолчать, чем дать слабый сигнал.\n\n"
        "Формат ответа СТРОГО, ровно два элемента, без цифр уровней в тексте:\n"
        "Строка 1: одно слово — LONG, SHORT или НЕТ_СИГНАЛА\n"
        "Строка 2 и далее: 1-2 коротких предложения обоснования (простой текст, "
        "без HTML-тегов, без markdown, без упоминания конкретных цифр входа/стопа/"
        "тейка — только качественное обоснование: тренд, объём, новости, риски). "
        "Держи ответ КОРОТКИМ — это один из нескольких тикеров в общей сводке."
    )

    lines = []
    lines.append("ТЕКУЩАЯ ЦЕНА " + ticker_label + ": $" + fmt_price(metrics["price"]))
    if metrics.get("ema50"):
        lines.append("EMA50 (4ч): $" + fmt_price(metrics["ema50"]))
    if metrics.get("ema200"):
        lines.append("EMA200 (4ч): $" + fmt_price(metrics["ema200"]))
    if metrics.get("rsi14") is not None:
        lines.append("RSI(14) на 4ч: " + "{:.1f}".format(metrics["rsi14"]))
    lines.append("Объём текущего 4ч бара к среднему: ×" + "{:.2f}".format(metrics["vol_ratio"]))
    lines.append("ATR(14, 4ч): $" + fmt_price(levels["atr"]))

    lines.append("")
    lines.append("ГОТОВЫЙ НАБОР УРОВНЕЙ LONG (посчитан точно, не меняй числа):")
    lines.append("Вход: $" + fmt_price(levels["long"]["entry"]))
    lines.append("Стоп: $" + fmt_price(levels["long"]["stop"]))
    lines.append("Тейк: $" + fmt_price(levels["long"]["take"]))
    if levels["long"]["rr"]:
        lines.append("R:R: 1:" + "{:.1f}".format(levels["long"]["rr"]))

    lines.append("")
    lines.append("ГОТОВЫЙ НАБОР УРОВНЕЙ SHORT (посчитан точно, не меняй числа):")
    lines.append("Вход: $" + fmt_price(levels["short"]["entry"]))
    lines.append("Стоп: $" + fmt_price(levels["short"]["stop"]))
    lines.append("Тейк: $" + fmt_price(levels["short"]["take"]))
    if levels["short"]["rr"]:
        lines.append("R:R: 1:" + "{:.1f}".format(levels["short"]["rr"]))

    lines.append("")
    lines.append("СВЕЖИЕ НОВОСТИ (общий крипторынок):")
    if news:
        for h in news:
            lines.append("- " + h)
    else:
        lines.append("(недоступны)")

    if whale_summary:
        lines.append("")
        lines.append("КИТОВАЯ АКТИВНОСТЬ: " + whale_summary)
    if volume_summary:
        lines.append("")
        lines.append("ОБЪЁМ: " + volume_summary)

    lines.append("")
    lines.append("ИСТОРИЯ СВЕЧЕЙ 4ч (последние ~7 дней, [время,open,high,low,close,volume]):")
    for row in rows_4h_tail:
        lines.append(str(row[:6]))

    user_prompt = "\n".join(lines)
    return system_prompt, user_prompt

def parse_trade_decision(raw_text):
    """Разбирает ответ Opus: первая непустая строка = решение (LONG/SHORT/НЕТ_СИГНАЛА
    в любом регистре, с пробелом или подчёркиванием), остальное = обоснование."""
    lines = [l.strip() for l in raw_text.strip().split("\n") if l.strip()]
    if not lines:
        return "НЕТ_СИГНАЛА", ""
    first = lines[0].upper().replace(" ", "_").replace("-", "_")
    if "LONG" in first:
        decision = "LONG"
    elif "SHORT" in first:
        decision = "SHORT"
    else:
        decision = "НЕТ_СИГНАЛА"
    reasoning = "\n".join(lines[1:]).strip()
    return decision, reasoning

def run_single_ticker_signal(coin):
    """Полный проход торгового модуля для ОДНОГО тикера: резолв символа -> свечи ->
    метрики -> уровни по формуле -> решение Opus (LONG/SHORT/нет сигнала) ->
    готовый текстовый блок для вставки в общее мультитикерное сообщение.
    Возвращает (block_text, error). Карточка с цифрами собирается кодом Python
    из levels, не из текста модели — та же архитектура, что и раньше для BTC."""
    symbol, category = resolve_symbol_and_category(coin)
    if not symbol:
        return None, "Тикер '" + coin + "' не найден на Bybit"

    rows_4h = get_klines_universal(symbol, category, "240", limit=200)
    if len(rows_4h) < 60:
        return None, "Недостаточно данных по свечам для " + symbol

    metrics = build_universal_metrics(rows_4h)
    if metrics is None:
        return None, "Не удалось посчитать метрики для " + symbol

    levels = build_trade_levels(metrics, rows_4h)
    if levels is None:
        return None, "Не удалось посчитать уровни для " + symbol + " (нет ATR или уровней поддержки/сопротивления)"

    news = get_crypto_news_headlines()
    whale_summary = get_last_whale_summary_text(coin)
    volume_summary = get_last_volume_summary_text(coin)
    rows_4h_tail = rows_4h[-42:]   # последние ~7 дней для контекста истории

    system_prompt, user_prompt = build_trade_signal_prompt(coin, metrics, levels, rows_4h_tail, news, whale_summary, volume_summary)
    raw_text, error = call_claude_opus(system_prompt, user_prompt, max_tokens=500)
    if error:
        return None, error

    decision, reasoning = parse_trade_decision(raw_text)

    if decision in ("LONG", "SHORT"):
        lv = levels["long"] if decision == "LONG" else levels["short"]
        dir_emoji = "&#128994;" if decision == "LONG" else "&#128308;"
        rr_text = "1:" + "{:.1f}".format(lv["rr"]) if lv["rr"] else "н/д"
        block = (
            "<b>" + coin + "</b>\n"
            + dir_emoji + " <b>" + decision + "</b>\n"
            + "Вход: $" + fmt_price(lv["entry"]) + "\n"
            + "Стоп: $" + fmt_price(lv["stop"]) + "\n"
            + "Тейк: $" + fmt_price(lv["take"]) + "\n"
            + "R:R: " + rr_text + "\n"
            + "Обоснование: " + reasoning
        )
    else:
        block = (
            "<b>" + coin + "</b>\n"
            + "&#9898; <b>СИГНАЛА НЕТ</b>\n"
            + "Обоснование: " + (reasoning or "Ситуация неоднозначная.")
        )

    return block, None

def run_trade_signal(send=True):
    """Проходит по всем тикерам из TRADE_SIGNAL_TICKERS (BTC, ETH, LINK), собирает
    каждый блок через run_single_ticker_signal и объединяет в ОДНО сообщение.
    Если по какому-то тикеру произошла ошибка — в его блоке будет короткая пометка
    об ошибке, остальные тикеры это не блокирует. Возвращает (text, error) —
    error не None только если ВСЕ тикеры не удалось обработать."""
    blocks = []
    any_success = False
    for coin in TRADE_SIGNAL_TICKERS:
        block, error = run_single_ticker_signal(coin)
        if block:
            blocks.append(block)
            any_success = True
        else:
            blocks.append("<b>" + coin + "</b>\n&#9888; " + (error or "неизвестная ошибка"))

    if not any_success:
        return None, "Не удалось получить сигнал ни по одному тикеру: " + "; ".join(blocks)

    line = "------------------------------"
    separator = "\n" + line + "\n"
    full_text = (
        "&#127919; <b>ТОРГОВЫЙ СИГНАЛ</b>\n"
        + datetime.now().strftime("%d.%m.%Y  %H:%M") + " (UTC+5)\n"
        + line + "\n"
        + separator.join(blocks) + "\n"
        + line + "\n"
        + "<i>Usoltsev Signals · " + CLAUDE_MODEL + "</i>"
    )

    if send:
        send_telegram(full_text, THREAD_GENERAL)

    return full_text, None

def trade_signal_loop():
    """Фоновый поток: проверяет каждую минуту, не наступил ли один из часов
    TRADE_SIGNAL_HOURS_UTC5 (7:00 / 14:00 / 18:00 / 22:00, UTC+5) — и если да,
    запускает мультитикерный сигнал один раз за эту минуту (защита от повторной
    отправки внутри той же минуты через флаг)."""
    last_sent_key = None
    while True:
        try:
            now = datetime.now()
            if now.hour in TRADE_SIGNAL_HOURS_UTC5 and now.minute == 0:
                key = now.strftime("%Y-%m-%d-%H")
                if key != last_sent_key:
                    run_trade_signal(send=True)
                    last_sent_key = key
        except:
            pass
        time.sleep(60)

# ═══════════════════════════════════════════════
# ASK ANALYSIS — анализ любого тикера по запросу (Claude Sonnet 5)
# ═══════════════════════════════════════════════
# Тема Telegram: THREAD_ASK_ANALYSIS. Любое сообщение в этой теме трактуется как
# запрос "ТИКЕР ТАЙМФРЕЙМ" (гибкий парсинг). Поддерживает крипто-перпетуалы Bybit
# (category=linear) и токенизированные акции xStocks (category=spot, суффикс x).
# Ответ: картинка (свечи + Volume Profile сбоку с POC/VAH/VAL) через mplfinance +
# короткий текстовый анализ от Sonnet 5 под подписью к фото.
THREAD_ASK_ANALYSIS = 2866

# Известные тикеры xStocks на Bybit (спот, суффикс x к тикеру: AAPL -> AAPLx)
XSTOCKS_TICKERS = {"AAPL", "TSLA", "NVDA", "AMZN", "META", "GOOGL", "GOOG",
                    "COIN", "MCD", "HOOD", "CRCL"}

# Таймфреймы: пользовательский ввод -> код интервала Bybit + подпись
TF_ALIASES = {
    "1m": "1", "1м": "1", "1min": "1",
    "5m": "5", "5м": "5",
    "15m": "15", "15м": "15",
    "30m": "30", "30м": "30",
    "1h": "60", "1ч": "60", "60": "60", "60m": "60",
    "2h": "120", "2ч": "120",
    "4h": "240", "4ч": "240", "240": "240",
    "8h": "480", "8ч": "480",
    "12h": "720", "12ч": "720",
    "1d": "D", "1д": "D", "d": "D", "day": "D", "дн": "D",
    "1w": "W", "1н": "W", "w": "W", "неделя": "W",
}
TF_CODE_LABEL = {
    "1": "1м", "5": "5м", "15": "15м", "30": "30м",
    "60": "1ч", "120": "2ч", "240": "4ч", "480": "8ч", "720": "12ч",
    "D": "1д", "W": "1нед",
}

def parse_analysis_request(text):
    """Парсит свободный текст вида "fartcoinusdt 4h", "HYPE - 4ч", "aapl, 1d" на
    (тикер_raw, tf_code). Разделитель — ЛЮБОЙ пробельный символ (включая
    неразрывный пробел U+00A0 и подобные, которые часто прилетают при копировании
    с телефона) плюс дефис/запятая, регистр не важен. Возвращает None, если не
    удалось распознать структуру."""
    if not text:
        return None
    cleaned = text.strip().lower()
    for sep in ["-", ",", "\u00a0", "\u200b", "\t"]:
        cleaned = cleaned.replace(sep, " ")
    # split() без аргумента разбивает по ЛЮБЫМ пробельным символам и схлопывает
    # повторы — надёжнее, чем split(" ") с ручной фильтрацией пустых элементов
    parts = cleaned.split()
    if len(parts) < 2:
        return None
    tf_raw = parts[-1]
    ticker_raw = "".join(parts[:-1])
    tf_code = TF_ALIASES.get(tf_raw)
    if not tf_code:
        return None
    if not ticker_raw or not ticker_raw.isalnum():
        return None
    return ticker_raw.upper(), tf_code

def debug_text_repr(text):
    """Для диагностики нераспознанных запросов — показывает точный код каждого
    символа сообщения, чтобы отличить обычный пробел от невидимых символов
    (неразрывный пробел, zero-width space и т.п.), которые ломают парсинг."""
    if not text:
        return "(пустая строка)"
    return " ".join("U+%04X(%r)" % (ord(ch), ch) for ch in text[:60])

def resolve_symbol_and_category(ticker_raw):
    """Определяет реальный тикер Bybit и category (linear для крипто-перпетуалов,
    spot для токенизированных акций xStocks). Реальный формат xStocks на Bybit —
    ЗАГЛАВНЫЙ суффикс X слитно с тикером, например TSLAXUSDT (не TSLAxUSDT) —
    подтверждено официальными объявлениями Bybit и live-тикером на TradingView.
    Как и для крипты, результат ПРОВЕРЯЕТСЯ реальным запросом к Bybit API, а не
    просто конструируется вслепую по шаблону — чтобы не наступить на ту же
    ошибку с регистром ещё раз, если формат для какого-то конкретного тикера
    вдруг будет отличаться."""
    base = ticker_raw.replace("USDT", "").replace("USD", "")
    # Пользователь может написать как короткое имя ("AAPL"), так и полный
    # реальный тикер xStocks с суффиксом ("AAPLXUSDT" -> после снятия USDT
    # остаётся "AAPLX") — проверяем оба варианта.
    base_no_x = base[:-1] if base.endswith("X") and base[:-1] in XSTOCKS_TICKERS else base

    if base_no_x in XSTOCKS_TICKERS:
        base = base_no_x
    if base in XSTOCKS_TICKERS:
        stock_candidate = base + "X" + "USDT"
        try:
            url = "https://api.bybit.com/v5/market/tickers?category=spot&symbol=" + stock_candidate
            r = requests.get(url, timeout=6).json()
            if r.get("result", {}).get("list"):
                return stock_candidate, "spot"
        except:
            pass
        # если по каким-то причинам не нашли даже с правильным регистром —
        # не падаем сразу, ниже всё равно попробуем как крипто-перпетуал

    # крипто: пробуем как обычный перпетуал
    for candidate in [ticker_raw, ticker_raw + "USDT" if not ticker_raw.endswith("USDT") else ticker_raw]:
        try:
            url = "https://api.bybit.com/v5/market/tickers?category=linear&symbol=" + candidate
            r = requests.get(url, timeout=6).json()
            if r.get("result", {}).get("list"):
                return candidate, "linear"
        except:
            continue
    return None, None

def get_klines_universal(symbol, category, interval, limit=200):
    """Свечи Bybit для произвольной категории (linear/spot). Возвращает список
    от старых к новым, как и get_btc_klines."""
    try:
        url = ("https://api.bybit.com/v5/market/kline?category=" + category
               + "&symbol=" + symbol + "&interval=" + str(interval) + "&limit=" + str(limit))
        r = requests.get(url, timeout=15).json()
        rows = r.get("result", {}).get("list", [])
        return list(reversed(rows))
    except:
        return []

def build_universal_metrics(rows):
    """Обобщённая версия build_btc_metrics для произвольного тикера/ТФ. Считает
    EMA50/EMA200 (если хватает баров), RSI14, объём к среднему, ближайшие уровни."""
    if len(rows) < 30:
        return None
    closes = [float(r[4]) for r in rows]
    vols   = [float(r[5]) for r in rows]

    ema50  = calc_ema(closes, min(50, len(closes) - 1))
    ema200 = calc_ema(closes, min(200, len(closes) - 1)) if len(closes) >= 60 else None
    rsi14  = calc_rsi(closes, 14)

    cur_vol = vols[-1]
    avg_vol = sum(vols[-21:-1]) / 20 if len(vols) >= 21 else sum(vols[:-1]) / max(len(vols) - 1, 1)
    vol_ratio = cur_vol / avg_vol if avg_vol > 0 else 0

    support, resistance, cur_price = find_support_resistance(rows, lookback=min(60, len(rows)))
    atr = calc_atr(rows, 14)

    return {
        "price": cur_price, "ema50": ema50, "ema200": ema200, "rsi14": rsi14,
        "vol_ratio": vol_ratio, "support": support, "resistance": resistance, "atr": atr,
    }

def find_key_order_blocks(rows, pivot_len=5):
    """Ищет ОДИН самый значимый (по объёму пивота) неотработанный Order Block
    с каждой стороны (bullish/bearish). Логика перенесена с Pine-версии:
    структура рынка определяется по пивотам ЦЕНЫ (highest/lowest за pivot_len),
    смена структуры вниз после серии хаёв формирует bullish OB на последнем
    "бычьем" баре перед разворотом, смена вверх после серии лоу — bearish OB.
    Митигирование: блок считается отработанным, если цена после его формирования
    уже прошла его целиком (закрытие ниже нижней границы для bullish, выше верхней
    для bearish) — такие блоки в отбор не попадают."""
    if len(rows) < pivot_len * 3:
        return None, None

    highs = [float(r[2]) for r in rows]
    lows  = [float(r[3]) for r in rows]
    opens = [float(r[1]) for r in rows]
    closes = [float(r[4]) for r in rows]
    vols  = [float(r[5]) for r in rows]
    n = len(rows)

    candidates_bull = []   # список (top, bottom, volume, bar_idx)
    candidates_bear = []

    for i in range(pivot_len, n - pivot_len):
        window_high = max(highs[i - pivot_len:i])
        window_low  = min(lows[i - pivot_len:i])
        # Смена структуры вниз: текущий хай пробивает недавний минимум window —
        # разворот вниз, ищем последний бычий (close > open) бар перед этим как OB
        if lows[i] < window_low:
            for j in range(i - 1, max(i - pivot_len - 1, -1), -1):
                if closes[j] > opens[j]:
                    # Bull OB зона: [low пивота, hl2 пивота] — как в оригинале Pine.
                    # Митигирование по close < low (не по середине — так было
                    # слишком хрупко, блоки пробивались почти сразу же).
                    top = (highs[j] + lows[j]) / 2
                    bottom = lows[j]
                    candidates_bull.append((top, bottom, vols[j], j))
                    break
        # Смена структуры вверх: текущий лоу пробивает недавний максимум window —
        # разворот вверх, ищем последний медвежий бар перед этим как OB
        if highs[i] > window_high:
            for j in range(i - 1, max(i - pivot_len - 1, -1), -1):
                if closes[j] < opens[j]:
                    # Bear OB зона: [hl2 пивота, high пивота]. Митигирование по
                    # close > high.
                    top = highs[j]
                    bottom = (highs[j] + lows[j]) / 2
                    candidates_bear.append((top, bottom, vols[j], j))
                    break

    def pick_best_unmitigated(candidates, is_bull):
        best = None
        for top, bottom, vol, j in candidates:
            mitigated = False
            for k in range(j + 1, n):
                if is_bull and closes[k] < bottom:
                    mitigated = True
                    break
                if not is_bull and closes[k] > top:
                    mitigated = True
                    break
            if mitigated:
                continue
            if best is None or vol > best[2]:
                best = (top, bottom, vol, j)
        return best

    best_bull = pick_best_unmitigated(candidates_bull, True)
    best_bear = pick_best_unmitigated(candidates_bear, False)

    bull_zone = {"top": best_bull[0], "bottom": best_bull[1]} if best_bull else None
    bear_zone = {"top": best_bear[0], "bottom": best_bear[1]} if best_bear else None
    return bull_zone, bear_zone

def calc_volume_profile(rows, bins=40):
    """Volume Profile на Python: распределяет объём каждого бара пропорционально
    по ценовым бинам между его low и high (упрощённая версия — без раздельного
    веса тела/теней, аналог weightBody=False в Pine). Возвращает (poc, vah, val,
    bin_edges, bin_volumes) для отрисовки боковой гистограммы."""
    highs = [float(r[2]) for r in rows]
    lows  = [float(r[3]) for r in rows]
    vols  = [float(r[5]) for r in rows]

    top = max(highs)
    btm = min(lows)
    if top <= btm:
        return None
    step = (top - btm) / bins
    bin_vols = [0.0] * bins

    for h, l, v in zip(highs, lows, vols):
        r1 = max(0, min(bins - 1, int((l - btm) / step)))
        r2 = max(0, min(bins - 1, int((h - btm) / step)))
        per = v / (r2 - r1 + 1)
        for i in range(r1, r2 + 1):
            bin_vols[i] += per

    total_v = sum(bin_vols)
    if total_v <= 0:
        return None
    poc_idx = bin_vols.index(max(bin_vols))
    poc = btm + step * (poc_idx + 0.5)

    up_i, dn_i = poc_idx, poc_idx
    va = bin_vols[poc_idx]
    while va < total_v * 0.7 and (up_i < bins - 1 or dn_i > 0):
        v_up = bin_vols[up_i + 1] if up_i < bins - 1 else -1
        v_dn = bin_vols[dn_i - 1] if dn_i > 0 else -1
        if v_up >= v_dn:
            up_i += 1
            va += max(v_up, 0)
        else:
            dn_i -= 1
            va += max(v_dn, 0)
    vah = btm + step * (up_i + 1)
    val = btm + step * dn_i

    bin_edges = [btm + step * i for i in range(bins + 1)]
    return {"poc": poc, "vah": vah, "val": val, "bin_edges": bin_edges, "bin_volumes": bin_vols}

def plot_analysis_chart(rows, metrics, profile, ticker_label, tf_label, order_blocks=None):
    """Рисует свечной график (mplfinance) с EMA50/200, горизонтальными POC/VAH/VAL,
    боковой гистограммой Volume Profile справа, и (если найдены) зонами Order
    Blocks — по одной самой значимой неотработанной зоне с каждой стороны.
    Сохраняет PNG во временный файл и возвращает путь к нему."""
    import mplfinance as mpf
    import pandas as pd
    import matplotlib.pyplot as plt
    import matplotlib.gridspec as gridspec

    df = pd.DataFrame(rows, columns=["time", "open", "high", "low", "close", "volume", "turnover"])
    df["time"] = pd.to_datetime(df["time"].astype(float), unit="ms")
    df = df.set_index("time")
    for col in ["open", "high", "low", "close", "volume"]:
        df[col] = df[col].astype(float)

    fig = plt.figure(figsize=(11, 6), facecolor="#0d1117")
    gs = gridspec.GridSpec(1, 5, figure=fig, wspace=0.02)
    ax_chart = fig.add_subplot(gs[0, 0:4])
    ax_profile = fig.add_subplot(gs[0, 4], sharey=ax_chart)

    # При использовании внешних осей (наш случай — свечи + отдельная боковая
    # панель профиля) каждый addplot ОБЯЗАН явно указывать свою ось через ax=,
    # иначе mplfinance выбрасывает ValueError при попытке смешать внешний ax
    # у mpf.plot() с addplot без собственной оси.
    addplots = []
    closes = df["close"].tolist()
    if metrics.get("ema50"):
        ema50_series = pd.Series(pd.Series(closes).ewm(span=min(50, len(closes) - 1), adjust=False).mean().values, index=df.index)
        addplots.append(mpf.make_addplot(ema50_series, ax=ax_chart, color="orange", width=1))
    if metrics.get("ema200") and len(closes) >= 60:
        ema200_series = pd.Series(pd.Series(closes).ewm(span=min(200, len(closes) - 1), adjust=False).mean().values, index=df.index)
        addplots.append(mpf.make_addplot(ema200_series, ax=ax_chart, color="purple", width=1))

    mc = mpf.make_marketcolors(up="#26a69a", down="#f23645", inherit=True)
    style = mpf.make_mpf_style(base_mpf_style="nightclouds", marketcolors=mc, facecolor="#0d1117", edgecolor="#0d1117", gridcolor="#222")

    mpf.plot(df, type="candle", ax=ax_chart, style=style, addplot=addplots, warn_too_much_data=1000)

    if profile:
        bin_centers = [(profile["bin_edges"][i] + profile["bin_edges"][i+1]) / 2 for i in range(len(profile["bin_volumes"]))]
        bin_height = profile["bin_edges"][1] - profile["bin_edges"][0]
        ax_profile.barh(bin_centers, profile["bin_volumes"], height=bin_height, color="#3b5ba5", alpha=0.85)
        ax_profile.set_facecolor("#0d1117")
        ax_profile.axis("off")

        for lvl, color, label in [(profile["poc"], "#ffeb3b", "POC"), (profile["vah"], "#ff5252", "VAH"), (profile["val"], "#ff5252", "VAL")]:
            ax_chart.axhline(lvl, color=color, linewidth=1, linestyle="--", alpha=0.8)

    if order_blocks:
        import matplotlib.patches as patches
        x_left = df.index[0]
        x_right = df.index[-1]
        bull_ob, bear_ob = order_blocks
        if bull_ob:
            # x в координатах осей (0..1 = вся ширина графика), y — реальные цены
            rect = patches.Rectangle(
                (0, bull_ob["bottom"]), 1, bull_ob["top"] - bull_ob["bottom"],
                facecolor="#2962ff", alpha=0.18, edgecolor="#2962ff", linewidth=1, zorder=0
            )
            rect.set_transform(ax_chart.get_yaxis_transform())
            ax_chart.add_patch(rect)
        if bear_ob:
            rect = patches.Rectangle(
                (0, bear_ob["bottom"]), 1, bear_ob["top"] - bear_ob["bottom"],
                facecolor="#f23645", alpha=0.18, edgecolor="#f23645", linewidth=1, zorder=0
            )
            rect.set_transform(ax_chart.get_yaxis_transform())
            ax_chart.add_patch(rect)

    ax_chart.set_title(ticker_label + " · " + tf_label, color="white", fontsize=13, loc="left")
    fig.patch.set_facecolor("#0d1117")

    path = "/tmp/ask_analysis_" + str(int(time.time())) + ".png"
    fig.savefig(path, facecolor=fig.get_facecolor(), bbox_inches="tight", dpi=110)
    plt.close(fig)
    return path

TELEGRAM_CAPTION_LIMIT = 1024   # жёсткий лимит Telegram на длину caption к фото

def send_telegram_photo(photo_path, caption, thread_id=None):
    """Отправка изображения в Telegram (sendPhoto). В отличие от прежней версии,
    ПРОВЕРЯЕТ ответ Telegram и возвращает (ok, error) — раньше ошибка (например
    превышение лимита в 1024 символа на caption) проглатывалась молча, и бот
    просто ничего не отправлял без единого следа в логах."""
    url = "https://api.telegram.org/bot" + BOT_TOKEN + "/sendPhoto"
    try:
        with open(photo_path, "rb") as f:
            files = {"photo": f}
            data = {"chat_id": GROUP_CHAT_ID, "caption": caption, "parse_mode": "HTML"}
            if thread_id is not None:
                data["message_thread_id"] = thread_id
            r = requests.post(url, data=data, files=files, timeout=30)
        if r.status_code != 200:
            return False, "Telegram sendPhoto HTTP " + str(r.status_code) + ": " + r.text[:300]
        return True, None
    except Exception as e:
        return False, "Ошибка отправки фото: " + type(e).__name__ + ": " + str(e)

def build_ask_analysis_prompt(ticker_label, tf_label, metrics, profile, bull_ob=None, bear_ob=None):
    """Промпт для короткого текстового анализа (Sonnet 5) на основе точных цифр,
    посчитанных Python — без картинки, картинка формируется отдельно кодом."""
    system_prompt = (
        "Ты — крипто/фондовый аналитик, дающий краткий комментарий по запрошенному "
        "инструменту для приватного Telegram-канала. Используй ТОЛЬКО данные, "
        "переданные ниже, не выдумывай цифры. Формат ответа: 3-5 коротких "
        "предложений простым текстом (без HTML, без markdown) — текущая структура "
        "тренда, что говорит объём, где ключевые уровни (POC/VAH/VAL, "
        "поддержка/сопротивление, и Order Block если есть — это зона, где стоит "
        "ожидать реакции цены), и краткий вывод о том, на что смотреть дальше. "
        "Без гарантий и рекомендаций 'покупай/продавай' — только описание картины."
    )
    lines = []
    lines.append(ticker_label + " · " + tf_label)
    lines.append("Цена: " + fmt_price(metrics["price"]))
    if metrics.get("ema50"):
        lines.append("EMA50: " + fmt_price(metrics["ema50"]))
    if metrics.get("ema200"):
        lines.append("EMA200: " + fmt_price(metrics["ema200"]))
    if metrics.get("rsi14") is not None:
        lines.append("RSI(14): " + "{:.1f}".format(metrics["rsi14"]))
    lines.append("Объём текущего бара к среднему: ×" + "{:.2f}".format(metrics["vol_ratio"]))
    if metrics.get("support"):
        lines.append("Ближайшая поддержка: " + fmt_price(metrics["support"]))
    if metrics.get("resistance"):
        lines.append("Ближайшее сопротивление: " + fmt_price(metrics["resistance"]))
    if profile:
        lines.append("POC: " + fmt_price(profile["poc"]))
        lines.append("VAH: " + fmt_price(profile["vah"]) + " / VAL: " + fmt_price(profile["val"]))
    if bull_ob:
        lines.append("Order Block (бычий, зона спроса): " + fmt_price(bull_ob["bottom"]) + "-" + fmt_price(bull_ob["top"]))
    if bear_ob:
        lines.append("Order Block (медвежий, зона предложения): " + fmt_price(bear_ob["bottom"]) + "-" + fmt_price(bear_ob["top"]))
    user_prompt = "\n".join(lines)
    return system_prompt, user_prompt

def call_claude_sonnet(system_prompt, user_prompt, max_tokens=500):
    """Вызов Anthropic API с моделью Sonnet 5 (дешевле Opus, используется только
    для интерактивного модуля Ask Analysis — расписанные модули остаются на Opus)."""
    if not ANTHROPIC_API_KEY:
        return None, "ANTHROPIC_API_KEY не задан"
    try:
        url = "https://api.anthropic.com/v1/messages"
        headers = {
            "x-api-key": ANTHROPIC_API_KEY,
            "anthropic-version": "2023-06-01",
            "content-type": "application/json",
        }
        payload = {
            "model": "claude-sonnet-5",
            "max_tokens": max_tokens,
            "system": system_prompt,
            "messages": [{"role": "user", "content": user_prompt}],
        }
        r = requests.post(url, headers=headers, json=payload, timeout=60)
        if r.status_code != 200:
            return None, "HTTP " + str(r.status_code) + ": " + r.text[:500]
        data = r.json()
        blocks = data.get("content", [])
        text = "\n".join(b.get("text", "") for b in blocks if b.get("type") == "text").strip()
        if not text:
            return None, "Пустой ответ от API"
        return text, None
    except requests.exceptions.Timeout:
        return None, "TIMEOUT"
    except Exception as e:
        return None, "OTHER: " + type(e).__name__ + ": " + str(e)

# Простая защита от спама: не чаще 1 запроса раз в 20 секунд от всей группы суммарно
_ask_analysis_lock = threading.Lock()
_ask_analysis_last_ts = 0
ASK_ANALYSIS_COOLDOWN_SEC = 20

def run_ask_analysis(raw_text):
    """Полный проход: парсинг запроса -> резолв тикера -> свечи -> метрики ->
    Volume Profile -> картинка -> текст от Sonnet -> отправка фото с подписью.
    Возвращает (ok, error) для диагностики."""
    global _ask_analysis_last_ts
    with _ask_analysis_lock:
        now_ts = time.time()
        if now_ts - _ask_analysis_last_ts < ASK_ANALYSIS_COOLDOWN_SEC:
            return False, "Слишком частые запросы, подождите немного"
        _ask_analysis_last_ts = now_ts

    parsed = parse_analysis_request(raw_text)
    if not parsed:
        return False, ("Не удалось распознать запрос. Формат: ТИКЕР ТАЙМФРЕЙМ, "
                        "например 'fartcoinusdt 4h' или 'aapl 1d'.\n"
                        "Диагностика символов: " + debug_text_repr(raw_text))

    ticker_raw, tf_code = parsed
    symbol, category = resolve_symbol_and_category(ticker_raw)
    if not symbol:
        return False, "Тикер '" + ticker_raw + "' не найден на Bybit"

    rows = get_klines_universal(symbol, category, tf_code, limit=200)
    if len(rows) < 30:
        return False, "Недостаточно данных по свечам для " + symbol

    metrics = build_universal_metrics(rows)
    if metrics is None:
        return False, "Не удалось посчитать метрики"

    profile = calc_volume_profile(rows, bins=40)
    bull_ob, bear_ob = find_key_order_blocks(rows, pivot_len=5)

    tf_label = TF_CODE_LABEL.get(tf_code, tf_code)
    ticker_label = ticker_raw

    try:
        chart_path = plot_analysis_chart(rows, metrics, profile, ticker_label, tf_label, order_blocks=(bull_ob, bear_ob))
    except Exception as e:
        return False, "Ошибка отрисовки графика: " + str(e)

    system_prompt, user_prompt = build_ask_analysis_prompt(ticker_label, tf_label, metrics, profile, bull_ob, bear_ob)
    text, error = call_claude_sonnet(system_prompt, user_prompt)
    if error:
        text = "(текстовый анализ недоступен: " + error + ")"

    line = "------------------------------"
    # Короткая подпись — только факты, всегда идёт вместе с фото
    short_caption_parts = [
        "<b>" + ticker_label + " · " + tf_label + "</b>",
        "Цена: " + fmt_price(metrics["price"]),
    ]
    if profile:
        short_caption_parts.append("POC " + fmt_price(profile["poc"])
                              + " · область " + fmt_price(profile["val"])
                              + "–" + fmt_price(profile["vah"]))
    if bull_ob:
        short_caption_parts.append("&#128994; OB (спрос): " + fmt_price(bull_ob["bottom"]) + "–" + fmt_price(bull_ob["top"]))
    if bear_ob:
        short_caption_parts.append("&#128308; OB (предложение): " + fmt_price(bear_ob["bottom"]) + "–" + fmt_price(bear_ob["top"]))
    short_caption = "\n".join(short_caption_parts)

    # Полная подпись с текстом анализа — используется, только если укладывается
    # в лимит Telegram (1024 символа на caption к фото). Если текст анализа
    # слишком длинный (частый случай для более "многословных" ответов Sonnet,
    # например по BTC) — фото уходит с короткой подписью, а полный текст
    # отправляется следующим отдельным сообщением через send_telegram.
    full_caption = short_caption + "\n" + line + "\n" + text
    if len(full_caption) <= TELEGRAM_CAPTION_LIMIT:
        caption_to_send = full_caption
        send_text_separately = None
    else:
        caption_to_send = short_caption
        send_text_separately = line + "\n" + text

    photo_ok, photo_error = send_telegram_photo(chart_path, caption_to_send, THREAD_ASK_ANALYSIS)
    try:
        os.remove(chart_path)
    except:
        pass

    if not photo_ok:
        return False, photo_error

    if send_text_separately:
        send_telegram(send_text_separately, THREAD_ASK_ANALYSIS)

    return True, None


# Фоновый поток отслеживания цен Bybit
price_thread = threading.Thread(target=price_tracker_loop, daemon=True)
price_thread.start()

# Фоновый поток отслеживания китовых сделок (крупные сделки на бирже)
whale_thread = threading.Thread(target=whale_tracker_loop, daemon=True)
whale_thread.start()

# Фоновый поток ончейн-китов (спотовые закупки Ethereum + Solana)
onchain_thread = threading.Thread(target=onchain_whale_loop, daemon=True)
onchain_thread.start()

# Фоновый поток алертов по всплескам объёма (наши тикеры)
volume_thread = threading.Thread(target=volume_alert_loop, daemon=True)
volume_thread.start()

# Фоновый поток анализа BTC (Claude Opus 5, раз в 8 часов)
btc_analysis_thread = threading.Thread(target=btc_analysis_loop, daemon=True)
btc_analysis_thread.start()

# Фоновый поток торговых сигналов BTC (Claude Opus 5, дважды в день: 6:00 и 18:00 UTC+5)
trade_signal_thread = threading.Thread(target=trade_signal_loop, daemon=True)
trade_signal_thread.start()

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
