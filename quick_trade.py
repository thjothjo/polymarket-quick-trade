#!/usr/bin/env python3
"""
quick_trade.py — 多市场一键交易面板

启动:  python quick_trade.py
打开:  http://localhost:8890

快捷键:
  ↑ = 买 UP    ↓ = 买 DOWN
  Q = 卖 UP ALL    W = 卖 DOWN ALL
  1-5 = 选金额 ($5/$10/$20/$50/$100)
"""

import asyncio
import json
import math
import os
import sys
import time
import threading
import webbrowser
from collections import deque
from datetime import datetime, timezone

import aiohttp
from aiohttp import web
import requests
from dotenv import load_dotenv
from web3 import Web3

load_dotenv()

# ── 多账号配置 ─────────────────────────────────────────────────────────────────
CHAIN_ID = 137
SIGNATURE_TYPE = 3  # Polymarket Deposit Wallet (POLY_1271, V2 新钱包类型)
CLOB_HTTP = "https://clob.polymarket.com"
CTF_ADDR = "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"

def _load_accounts():
    """从 .env 加载所有 QUICK 账号: QUICK_PRIVATE_KEY / QUICK_FUNDER (第1个),
    QUICK_PRIVATE_KEY_2 / QUICK_FUNDER_2 (第2个), ..."""
    accts = []
    # 第 1 个 (无编号)
    pk = os.getenv("QUICK_PRIVATE_KEY", "")
    fd = os.getenv("QUICK_FUNDER", "")
    rk = os.getenv("QUICK_RELAYER_API_KEY", "")
    ra = os.getenv("QUICK_RELAYER_API_KEY_ADDRESS", "")
    if pk and fd:
        accts.append({"private_key": pk, "funder": fd,
                      "relayer_key": rk, "relayer_addr": ra,
                      "label": fd[:6] + ".." + fd[-4:]})
    # 第 2~N 个 (有编号, 兼容 _2 和 _02 格式)
    for i in range(2, 20):
        pk = os.getenv(f"QUICK_PRIVATE_KEY_{i}", "") or os.getenv(f"QUICK_PRIVATE_KEY_{i:02d}", "")
        fd = os.getenv(f"QUICK_FUNDER_{i}", "") or os.getenv(f"QUICK_FUNDER_{i:02d}", "")
        rk = os.getenv(f"QUICK_RELAYER_API_KEY_{i}", "") or os.getenv(f"QUICK_RELAYER_API_KEY_{i:02d}", "")
        ra = os.getenv(f"QUICK_RELAYER_API_KEY_ADDRESS_{i}", "") or os.getenv(f"QUICK_RELAYER_API_KEY_ADDRESS_{i:02d}", "")
        if pk and fd:
            accts.append({"private_key": pk, "funder": fd,
                          "relayer_key": rk, "relayer_addr": ra,
                          "label": fd[:6] + ".." + fd[-4:]})
        else:
            break
    return accts

ACCOUNTS = _load_accounts()
_active_account_idx = 0
_account_lock = threading.Lock()

# 便捷访问当前账号
def _current_account():
    with _account_lock:
        idx = _active_account_idx
    if idx < len(ACCOUNTS):
        return ACCOUNTS[idx]
    return {"private_key": "", "funder": "", "relayer_key": "", "relayer_addr": "", "label": "N/A"}

# 兼容旧代码
PRIVATE_KEY = ACCOUNTS[0]["private_key"] if ACCOUNTS else ""
FUNDER = ACCOUNTS[0]["funder"] if ACCOUNTS else ""

# ── 多市场配置 ─────────────────────────────────────────────────────────────────
MARKETS = {
    "btc_5m":  {"label": "BTC 5m",  "slug_prefix": "btc-updown-5m",  "duration": 300,  "symbol": "BTC", "variant": "fiveminute",    "rtds": "btc/usd"},
    "btc_15m": {"label": "BTC 15m", "slug_prefix": "btc-updown-15m", "duration": 900,  "symbol": "BTC", "variant": "fifteenminute", "rtds": "btc/usd"},
    "btc_1h":  {"label": "BTC 1h",  "slug_prefix": "bitcoin",        "duration": 3600, "symbol": "BTC", "variant": "onehour",       "rtds": "btc/usd", "slug_type": "hourly"},
    "eth_5m":  {"label": "ETH 5m",  "slug_prefix": "eth-updown-5m",  "duration": 300,  "symbol": "ETH", "variant": "fiveminute",    "rtds": "eth/usd"},
    "eth_15m": {"label": "ETH 15m", "slug_prefix": "eth-updown-15m", "duration": 900,  "symbol": "ETH", "variant": "fifteenminute", "rtds": "eth/usd"},
    "eth_1h":  {"label": "ETH 1h",  "slug_prefix": "ethereum",       "duration": 3600, "symbol": "ETH", "variant": "onehour",       "rtds": "eth/usd", "slug_type": "hourly"},
}
active_market = "btc_5m"
active_market_lock = threading.Lock()
PORT = 8890

ALCHEMY_KEY = os.getenv("ALCHEMY_KEY", "")
RPC_URL = f"https://polygon-mainnet.g.alchemy.com/v2/{ALCHEMY_KEY}"

POLY_BUILDER_CODE = os.getenv("POLY_BUILDER_CODE", "0x0000000000000000000000000000000000000000000000000000000000000000")

# 滑点配置
BUY_SLIPPAGE = 0.10   # 买入滑点 (10 cents)
SELL_SLIPPAGE = 0.10   # 卖出滑点 (10 cents)

# ── 全局状态 ──────────────────────────────────────────────────────────────────
state_lock = threading.Lock()
state = {
    "round_ts": 0,
    "round_end": 0,
    "token_ids": {},
    "strike": 0.0,
    "crypto_current": 0.0,  # BTC 或 ETH 现价
    "up_price": 0.0,
    "down_price": 0.0,
    "up_ask": 0.0,
    "down_ask": 0.0,
    "up_position": 0.0,
    "down_position": 0.0,
    "usdc_balance": 0.0,
    "keys_ok": bool(ACCOUNTS),
    "up_book": {"bids": [], "asks": []},
    "down_book": {"bids": [], "asks": []},
}
# RTDS 推送缓存: symbol -> price
_rtds_prices = {"btc/usd": 0.0, "eth/usd": 0.0}
trade_log = deque(maxlen=50)


# ── Web3 连接 ─────────────────────────────────────────────────────────────────
_w3 = None

def get_w3():
    global _w3
    if _w3:
        return _w3
    try:
        from web3.middleware import ExtraDataToPOAMiddleware as POA
    except ImportError:
        try:
            from web3.middleware import geth_poa_middleware as POA
        except ImportError:
            POA = None
    _w3 = Web3(Web3.HTTPProvider(RPC_URL, request_kwargs={"timeout": 10}))
    if POA:
        try:
            _w3.middleware_onion.inject(POA, layer=0)
        except Exception:
            pass
    return _w3


# ── CLOB Client (多账号, 延迟初始化, 凭证缓存) ─────────────────────────────────
_clob = None
_clob_lock = threading.Lock()
_clob_account_idx = -1  # 当前 _clob 对应的账号索引
_CREDS_DIR = os.path.dirname(os.path.abspath(__file__))

def _creds_file_for(funder):
    return os.path.join(_CREDS_DIR, f".quick_trade_creds_{funder[:8].lower()}.json")

def _load_cached_creds(funder):
    """从本地文件加载缓存的 API 凭证"""
    try:
        path = _creds_file_for(funder)
        if not os.path.exists(path):
            return None
        with open(path, "r") as f:
            data = json.load(f)
        if data.get("funder") != funder:
            return None
        from py_clob_client_v2.clob_types import ApiCreds
        return ApiCreds(
            api_key=data["api_key"],
            api_secret=data["api_secret"],
            api_passphrase=data["api_passphrase"],
        )
    except Exception:
        return None

def _save_creds(funder, creds):
    """缓存 API 凭证到本地文件"""
    try:
        data = {
            "funder": funder,
            "api_key": creds.api_key,
            "api_secret": creds.api_secret,
            "api_passphrase": creds.api_passphrase,
        }
        with open(_creds_file_for(funder), "w") as f:
            json.dump(data, f)
    except Exception:
        pass

def get_clob_client():
    global _clob, _clob_account_idx
    acct = _current_account()
    pk, funder = acct["private_key"], acct["funder"]
    with _account_lock:
        idx = _active_account_idx
    with _clob_lock:
        if _clob is not None and _clob_account_idx == idx:
            return _clob
        if not pk or not funder:
            raise RuntimeError("账号未配置")
        from py_clob_client_v2.client import ClobClient
        _clob = ClobClient(
            host=CLOB_HTTP,
            key=pk,
            chain_id=CHAIN_ID,
            signature_type=SIGNATURE_TYPE,
            funder=funder,
        )
        cached = _load_cached_creds(funder)
        if cached:
            _clob.set_api_creds(cached)
            print(f"✅ CLOB client [{acct['label']}] (缓存凭证)")
        else:
            creds = _clob.create_or_derive_api_key()
            _clob.set_api_creds(creds)
            _save_creds(funder, creds)
            print(f"✅ CLOB client [{acct['label']}] (首次签名, 已缓存)")
        _clob_account_idx = idx
        return _clob


# ── 回合信息 ──────────────────────────────────────────────────────────────────
def _make_hourly_slug(crypto_name: str, round_ts: int) -> str:
    """生成 1h 市场的 slug, 如 bitcoin-up-or-down-april-25-2026-11am-et"""
    from datetime import timedelta
    # round_ts 是 UTC 整小时, 转换为 ET (UTC-4)
    et_dt = datetime.fromtimestamp(round_ts, tz=timezone.utc) - timedelta(hours=4)
    month = et_dt.strftime("%B").lower()  # april
    day = et_dt.day
    year = et_dt.year
    hour = et_dt.hour
    ampm = "am" if hour < 12 else "pm"
    h12 = hour % 12
    if h12 == 0:
        h12 = 12
    return f"{crypto_name}-up-or-down-{month}-{day}-{year}-{h12}{ampm}-et"


def get_round_token_ids(round_ts: int, slug_prefix: str = "btc-updown-5m", slug_type: str = "") -> dict:
    """获取回合的 UP/DOWN token_id"""
    if slug_type == "hourly":
        slug = _make_hourly_slug(slug_prefix, round_ts)
    else:
        slug = f"{slug_prefix}-{round_ts}"
    try:
        r = requests.get(
            "https://gamma-api.polymarket.com/events",
            params={"slug": slug}, timeout=10,
        )
        events = r.json()
        if not events or not isinstance(events, list):
            return {}
        mkt = events[0].get("markets", [{}])[0]
        tokens = mkt.get("clobTokenIds", "[]")
        outcomes = mkt.get("outcomes", "[]")
        if isinstance(tokens, str):
            tokens = json.loads(tokens)
        if isinstance(outcomes, str):
            outcomes = json.loads(outcomes)
        result = {}
        for i, o in enumerate(outcomes):
            if i < len(tokens):
                result[o.lower()] = tokens[i]
        return result
    except Exception:
        return {}


def _robust_get_json(url, timeout=5, headers=None) -> dict:
    """
    发送 GET 请求并解析为 JSON。
    首先尝试使用 requests.get，若失败或遭遇非 200 状态码，
    则自动 fallback 使用系统 curl 命令抓取以绕过 Cloudflare WAF 拦截。
    """
    try:
        r = requests.get(url, timeout=timeout, verify=False, headers=headers)
        if r.status_code == 200:
            return r.json()
    except Exception:
        pass

    # Fallback: 使用系统 curl
    import subprocess, json
    try:
        headers_list = []
        if headers:
            for k, v in headers.items():
                headers_list.extend(["-H", f"{k}: {v}"])
        cmd = ["curl", "-s"] + headers_list + [url]
        res = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if res.returncode == 0:
            return json.loads(res.stdout.strip())
    except Exception:
        pass
    return None


def get_strike_price(round_ts: int, duration: int = 300, symbol: str = "BTC", variant: str = "fiveminute") -> tuple:
    """获取本回合的锚定价。
    返回 (price, confirmed): confirmed=True 表示数据已稳定可缓存。
    注: 统一用 variant=fiveminute, 因为 fifteenminute/onehour variant
    返回的是整点 openPrice 而非该回合真实起始价。
    """
    try:
        start = datetime.fromtimestamp(round_ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        end = datetime.fromtimestamp(round_ts + duration, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        url = (f"https://polymarket.com/api/crypto/crypto-price?symbol={symbol}"
               f"&eventStartTime={start}&variant=fiveminute&endDate={end}")
        data = _robust_get_json(url, timeout=5, headers={"User-Agent": "Mozilla/5.0"})
        if data:
            op = data.get("openPrice")
            incomplete = data.get("incomplete", False)
            if op is not None and float(op) > 0:
                # incomplete=True 表示数据可能不准, 需要继续重试
                return float(op), not incomplete
    except Exception:
        pass
    return 0, False


# ── Token 余额查询 ───────────────────────────────────────────────────────────
_ctf_abi = json.loads(
    '[{"inputs":[{"name":"account","type":"address"},{"name":"id","type":"uint256"}],'
    '"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],'
    '"type":"function","constant":true}]'
)

def get_token_balance(w3, token_id: str) -> float:
    """查询当前账号 CTF token 持仓 (shares)"""
    try:
        funder = _current_account()["funder"]
        ctf = w3.eth.contract(address=w3.to_checksum_address(CTF_ADDR), abi=_ctf_abi)
        bal = ctf.functions.balanceOf(w3.to_checksum_address(funder), int(token_id)).call()
        return bal / 1e6
    except Exception:
        return 0


# ══════════════════════════════════════════════════════════════════════════════
# 交易执行
# ══════════════════════════════════════════════════════════════════════════════

def execute_buy(side: str, amount_usd: float) -> dict:
    """市价买入 (FOK)"""
    ts = time.time()
    with state_lock:
        token_id = state["token_ids"].get(side)
        ask = state.get(f"{side}_ask", 0)
        bid = state.get(f"{side}_price", 0)

    if not token_id:
        return {"success": False, "message": "回合未就绪，无 token"}

    price = ask if ask > 0 else bid
    if price <= 0:
        return {"success": False, "message": "无可用盘口价格"}

    try:
        client = get_clob_client()
        from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import BUY

        buy_price = round(min(price + BUY_SLIPPAGE, 0.99), 2)

        args = MarketOrderArgs(
            token_id=token_id,
            amount=round(amount_usd, 2),
            price=buy_price,
            side=BUY,
            order_type=OrderType.FOK,
            builder_code=POLY_BUILDER_CODE,
        )

        signed = client.create_market_order(args)
        resp = client.post_order(signed, OrderType.FOK)

        elapsed_ms = int((time.time() - ts) * 1000)
        order_id = resp.get("orderID", resp.get("id", ""))
        shares = round(amount_usd / price, 1)

        msg = f"BUY {side.upper()} {shares}sh @ {price:.2f} (${amount_usd})"
        _log_trade(True, msg, elapsed_ms)
        return {"success": True, "message": msg, "order_id": order_id, "elapsed_ms": elapsed_ms}

    except Exception as e:
        elapsed_ms = int((time.time() - ts) * 1000)
        msg = f"BUY {side.upper()} 失败: {str(e)[:100]}"
        _log_trade(False, msg, elapsed_ms)
        return {"success": False, "message": msg, "elapsed_ms": elapsed_ms}


def execute_sell(side: str, amount) -> dict:
    """市价卖出 (FOK). amount='all' 或 USD 金额."""
    ts = time.time()
    with state_lock:
        token_id = state["token_ids"].get(side)
        bid = state.get(f"{side}_price", 0)
        position = state.get(f"{side}_position", 0)

    if not token_id:
        return {"success": False, "message": "回合未就绪，无 token"}
    if position <= 0.01:
        return {"success": False, "message": "无持仓可卖"}

    # 确定卖出份额
    if amount == "all":
        # 从链上读取最新余额，避免缓存不准
        w3 = get_w3()
        actual = get_token_balance(w3, token_id)
        shares = math.floor(actual * 100) / 100
    else:
        usd = float(amount)
        price = bid if bid > 0 else 0.5
        shares = usd / price
        shares = min(shares, position)
        shares = math.floor(shares * 100) / 100

    if shares <= 0:
        return {"success": False, "message": "可卖份额为 0"}

    try:
        client = get_clob_client()
        from py_clob_client_v2.clob_types import MarketOrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import SELL

        sell_price = round(max((bid if bid > 0 else 0.5) - SELL_SLIPPAGE, 0.01), 2)

        args = MarketOrderArgs(
            token_id=token_id,
            amount=round(shares, 2),
            price=sell_price,
            side=SELL,
            order_type=OrderType.FOK,
            builder_code=POLY_BUILDER_CODE,
        )

        signed = client.create_market_order(args)
        resp = client.post_order(signed, OrderType.FOK)

        elapsed_ms = int((time.time() - ts) * 1000)
        order_id = resp.get("orderID", resp.get("id", ""))

        label = "ALL " if amount == "all" else ""
        msg = f"SELL {label}{side.upper()} {shares:.1f}sh @ {sell_price:.2f}"
        _log_trade(True, msg, elapsed_ms)
        return {"success": True, "message": msg, "order_id": order_id, "elapsed_ms": elapsed_ms}

    except Exception as e:
        elapsed_ms = int((time.time() - ts) * 1000)
        msg = f"SELL {side.upper()} 失败: {str(e)[:100]}"
        _log_trade(False, msg, elapsed_ms)
        return {"success": False, "message": msg, "elapsed_ms": elapsed_ms}


def _log_trade(success: bool, message: str, elapsed_ms: int):
    """添加交易记录"""
    trade_log.appendleft({
        "time": datetime.now().strftime("%H:%M:%S"),
        "success": success,
        "message": message,
        "elapsed_ms": elapsed_ms,
    })


def handle_trade(data: dict) -> dict:
    """统一交易入口"""
    action = data.get("action", "")
    side = data.get("side", "")
    amount = data.get("amount", 20)

    # cancel_all 不需要 side
    if action == "cancel_all_orders":
        return execute_cancel_all()

    if side not in ("up", "down"):
        return {"success": False, "message": "无效方向"}

    if action == "buy":
        return execute_buy(side, float(amount))
    elif action == "sell":
        return execute_sell(side, amount)
    elif action == "sell_all":
        return execute_sell(side, "all")
    elif action in ("limit_buy", "limit_sell"):
        limit_price = data.get("price", 0)
        if not limit_price or float(limit_price) <= 0:
            return {"success": False, "message": "请输入限价"}
        return execute_limit_order(
            side=side,
            buy_or_sell="buy" if action == "limit_buy" else "sell",
            limit_price=float(limit_price),
            amount_usd=float(amount),
        )
    else:
        return {"success": False, "message": f"未知操作: {action}"}


def execute_cancel_all() -> dict:
    """取消所有挂单"""
    ts = time.time()
    try:
        client = get_clob_client()
        resp = client.cancel_all()
        elapsed_ms = int((time.time() - ts) * 1000)
        # resp 通常返回 {"canceled": [...], "not_canceled": [...]}
        canceled = resp.get("canceled", []) if isinstance(resp, dict) else []
        count = len(canceled) if canceled else 0
        msg = f"已取消 {count} 笔挂单"
        _log_trade(True, msg, elapsed_ms)
        return {"success": True, "message": msg, "elapsed_ms": elapsed_ms}
    except Exception as e:
        elapsed_ms = int((time.time() - ts) * 1000)
        msg = f"取消挂单失败: {str(e)[:100]}"
        _log_trade(False, msg, elapsed_ms)
        return {"success": False, "message": msg, "elapsed_ms": elapsed_ms}


def execute_limit_order(side: str, buy_or_sell: str, limit_price: float, amount_usd: float) -> dict:
    """GTC 限价单"""
    ts = time.time()
    with state_lock:
        token_id = state["token_ids"].get(side)

    if not token_id:
        return {"success": False, "message": "回合未就绪，无 token"}

    limit_price = round(limit_price, 2)
    if limit_price <= 0 or limit_price >= 1:
        return {"success": False, "message": f"价格 {limit_price} 无效 (需 0.01-0.99)"}

    # USD → shares
    shares = round(amount_usd / limit_price, 2)
    if shares <= 0:
        return {"success": False, "message": "份额为 0"}

    try:
        client = get_clob_client()
        from py_clob_client_v2.clob_types import OrderArgs, OrderType
        from py_clob_client_v2.order_builder.constants import BUY, SELL

        args = OrderArgs(
            token_id=token_id,
            price=limit_price,
            size=shares,
            side=BUY if buy_or_sell == "buy" else SELL,
            builder_code=POLY_BUILDER_CODE,
        )

        signed = client.create_order(args)
        resp = client.post_order(signed, OrderType.GTC)

        elapsed_ms = int((time.time() - ts) * 1000)
        order_id = resp.get("orderID", resp.get("id", ""))

        label = "BUY" if buy_or_sell == "buy" else "SELL"
        msg = f"LIMIT {label} {side.upper()} {shares:.1f}sh @ {limit_price:.2f} (${amount_usd})"
        _log_trade(True, msg, elapsed_ms)
        return {"success": True, "message": msg, "order_id": order_id, "elapsed_ms": elapsed_ms}

    except Exception as e:
        elapsed_ms = int((time.time() - ts) * 1000)
        label = "BUY" if buy_or_sell == "buy" else "SELL"
        msg = f"LIMIT {label} {side.upper()} 失败: {str(e)[:100]}"
        _log_trade(False, msg, elapsed_ms)
        return {"success": False, "message": msg, "elapsed_ms": elapsed_ms}


# ══════════════════════════════════════════════════════════════════════════════
# 后台工作线程
# ══════════════════════════════════════════════════════════════════════════════

def state_worker():
    """后台线程: 回合管理、锚定价、持仓、余额 (每 3 秒, 慢速任务)"""
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    last_round = 0
    strike_confirmed_round = 0
    last_market_key = ""

    while True:
        try:
            with active_market_lock:
                mkt_key = active_market
            mkt = MARKETS[mkt_key]
            duration = mkt["duration"]

            # 市场切换 → 强制刷新
            if mkt_key != last_market_key:
                last_round = 0
                strike_confirmed_round = 0
                last_market_key = mkt_key

            # 更新 RTDS → state.crypto_current
            rtds_key = mkt["rtds"]
            with state_lock:
                state["crypto_current"] = _rtds_prices.get(rtds_key, 0.0)

            now = int(time.time())
            round_ts = now - (now % duration)

            # ── 新回合检测 ──
            if round_ts != last_round:
                token_ids = get_round_token_ids(round_ts, mkt["slug_prefix"], mkt.get("slug_type", ""))
                if token_ids:
                    with state_lock:
                        state["round_ts"] = round_ts
                        state["round_end"] = round_ts + duration
                        state["token_ids"] = token_ids
                        state["up_position"] = 0
                        state["down_position"] = 0
                        state["strike"] = 0
                    last_round = round_ts
                    rt = datetime.fromtimestamp(round_ts).strftime("%H:%M:%S")
                    print(f"🔄 [{mkt['label']}] 新回合 {rt} | 等待锚定价...")

            # ── 锚定价 (持续重试) ──
            with state_lock:
                current_round = state["round_ts"]

            strike_pending = False
            if current_round > 0 and strike_confirmed_round != current_round:
                strike_pending = True
                strike, confirmed = get_strike_price(current_round, duration, mkt["symbol"], mkt["variant"])
                if strike > 0:
                    with state_lock:
                        state["strike"] = strike
                    if confirmed:
                        strike_confirmed_round = current_round
                        rt = datetime.fromtimestamp(current_round).strftime("%H:%M:%S")
                        print(f"📌 [{mkt['label']}] 回合 {rt} 锚定价: ${strike:,.2f}")
                        strike_pending = False
                    elif not hasattr(state_worker, '_strike_notified') or state_worker._strike_notified != current_round:
                        state_worker._strike_notified = current_round
                        rt = datetime.fromtimestamp(current_round).strftime("%H:%M:%S")
                        print(f"⏳ [{mkt['label']}] 回合 {rt} 锚定价(未确认): ${strike:,.2f}")


            # ── 资产查询 (pUSD 和持仓, 走 CLOB API 不消耗 Alchemy CU, 每5秒刷新) ──
            with state_lock:
                tids = dict(state["token_ids"])
            acct = _current_account()
            _now = time.time()
            if not hasattr(state_worker, '_last_bal_t'):
                state_worker._last_bal_t = 0
                state_worker._last_bal_idx = -1
            # 切换账号后立即刷新余额
            with _account_lock:
                cur_idx = _active_account_idx
            acct_changed = (cur_idx != getattr(state_worker, '_last_bal_idx', -1))
            if acct["private_key"] and acct["funder"] and (acct_changed or _now - state_worker._last_bal_t >= 5):
                state_worker._last_bal_t = _now
                state_worker._last_bal_idx = cur_idx
                try:
                    from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
                    client = get_clob_client()
                    # 1. 查询 pUSD 余额
                    data = client.get_balance_allowance(
                        BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
                    )
                    if isinstance(data, dict):
                        b = data.get("balance", 0)
                        if not b:
                            b = data.get("collateral", {}).get("balance", 0)
                        with state_lock:
                            state["usdc_balance"] = float(b) / 1e6 if b else 0
                    
                    # 2. 查询持仓 (Token ID)
                    for side in ("up", "down"):
                        tid = tids.get(side)
                        if tid:
                            c_data = client.get_balance_allowance(
                                BalanceAllowanceParams(asset_type=AssetType.CONDITIONAL, token_id=str(tid))
                            )
                            if isinstance(c_data, dict):
                                cb = c_data.get("balance", 0)
                                with state_lock:
                                    state[f"{side}_position"] = float(cb) / 1e6 if cb else 0
                except Exception:
                    pass

        except Exception as e:
            print(f"[state_worker] 错误: {e}")

        # 锚定价待确认时加快轮询
        time.sleep(1 if strike_pending else 3)


def clob_price_worker():
    """后台线程: 高频 CLOB 盘口价轮询 (每秒, 独立线程)"""
    session = requests.Session()
    session.headers.update({"Accept": "application/json"})

    while True:
        try:
            with state_lock:
                tids = dict(state["token_ids"])

            for side in ("up", "down"):
                tid = tids.get(side)
                if not tid:
                    continue
                try:
                    r = session.get(
                        f"{CLOB_HTTP}/book",
                        params={"token_id": tid},
                        timeout=2,
                    )
                    if r.status_code == 200:
                        book = r.json()
                        bids = book.get("bids", [])
                        asks = book.get("asks", [])
                        best_bid = max((float(b["price"]) for b in bids), default=0) if bids else 0
                        best_ask = min((float(a["price"]) for a in asks), default=0) if asks else 0
                        # Top-5 orderbook
                        sorted_bids = sorted(bids, key=lambda b: float(b["price"]), reverse=True)[:10]
                        sorted_asks = sorted(asks, key=lambda a: float(a["price"]))[:10]
                        top_bids = [{"p": float(b["price"]), "s": float(b["size"])} for b in sorted_bids]
                        top_asks = [{"p": float(a["price"]), "s": float(a["size"])} for a in sorted_asks]
                        with state_lock:
                            state[f"{side}_price"] = best_bid
                            state[f"{side}_ask"] = best_ask
                            state[f"{side}_book"] = {"bids": top_bids, "asks": top_asks}
                except Exception:
                    pass
        except Exception:
            pass

        time.sleep(0.1)


# 嵌入模式标志 (由 onchain_leaderboard 设置)
_embedded = False



# ══════════════════════════════════════════════════════════════════════════════
# aiohttp Web 应用
# ══════════════════════════════════════════════════════════════════════════════

app = web.Application()


async def index(request):
    return web.Response(text=HTML_TEMPLATE, content_type="text/html", charset="utf-8")


async def trade_api(request):
    data = await request.json()
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, handle_trade, data)
    return web.json_response(result)


async def switch_market_api(request):
    global active_market
    data = await request.json()
    mkt_key = data.get("market", "")
    if mkt_key not in MARKETS:
        return web.json_response({"success": False, "message": f"未知市场: {mkt_key}"})
    with active_market_lock:
        active_market = mkt_key
    # 清空 state 让 worker 立即刷新
    with state_lock:
        state["round_ts"] = 0
        state["round_end"] = 0
        state["token_ids"] = {}
        state["strike"] = 0
        state["up_price"] = 0
        state["down_price"] = 0
        state["up_ask"] = 0
        state["down_ask"] = 0
        state["up_position"] = 0
        state["down_position"] = 0
    label = MARKETS[mkt_key]["label"]
    print(f"🔀 切换市场 → {label}")
    return web.json_response({"success": True, "message": f"已切换到 {label}"})


async def switch_account_api(request):
    global _active_account_idx
    data = await request.json()
    idx = data.get("index", 0)
    if idx < 0 or idx >= len(ACCOUNTS):
        return web.json_response({"success": False, "message": f"无效账号索引: {idx}"})
    with _account_lock:
        _active_account_idx = idx
    acct = ACCOUNTS[idx]
    # 同步 relayer 环境变量
    if acct.get("relayer_key"):
        os.environ["RELAYER_API_KEY"] = acct["relayer_key"]
    if acct.get("relayer_addr"):
        os.environ["RELAYER_API_KEY_ADDRESS"] = acct["relayer_addr"]
    # 清空持仓 & 余额 (让 worker 重新查询)
    with state_lock:
        state["up_position"] = 0
        state["down_position"] = 0
        state["usdc_balance"] = 0
    print(f"👛 切换账号 → [{acct['label']}]")
    # 后台初始化新账号的 CLOB client
    def _init():
        try:
            get_clob_client()
        except Exception as e:
            print(f"  ❌ CLOB 初始化失败: {e}")
    threading.Thread(target=_init, daemon=True).start()
    return web.json_response({"success": True, "message": f"已切换到 {acct['label']}"})


async def ws_handler(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    try:
        while not ws.closed:
            with state_lock:
                data = dict(state)
            with active_market_lock:
                data["active_market"] = active_market
            data["market_label"] = MARKETS.get(active_market, {}).get("label", "")
            data["trades"] = list(trade_log)
            data["server_time"] = time.time()
            # 账号信息
            with _account_lock:
                data["active_account"] = _active_account_idx
            data["accounts"] = [a["label"] for a in ACCOUNTS]
            await ws.send_json(data)
            await asyncio.sleep(0.1)
    except Exception:
        pass
    return ws


app.router.add_get("/", index)
app.router.add_post("/api/trade", trade_api)
app.router.add_post("/api/switch_market", switch_market_api)
app.router.add_post("/api/switch_account", switch_account_api)
app.router.add_get("/ws", ws_handler)



# ══════════════════════════════════════════════════════════════════════════════
# HTML 模板 (嵌入式, 单文件部署)
# ══════════════════════════════════════════════════════════════════════════════

HTML_TEMPLATE = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>⚡ Quick Trade</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;600;700&display=swap" rel="stylesheet">
<style>
:root {
    --bg: #080c15;
    --surface: rgba(15, 20, 35, 0.95);
    --surface-hover: rgba(25, 32, 52, 0.95);
    --border: rgba(100, 116, 139, 0.15);
    --text: #e2e8f0;
    --text-muted: #64748b;
    --up: #22c55e;
    --up-dark: #15803d;
    --up-dim: rgba(34, 197, 94, 0.12);
    --up-glow: rgba(34, 197, 94, 0.4);
    --down: #ef4444;
    --down-dark: #b91c1c;
    --down-dim: rgba(239, 68, 68, 0.12);
    --down-glow: rgba(239, 68, 68, 0.4);
    --blue: #3b82f6;
    --blue-dim: rgba(59, 130, 246, 0.15);
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
    font-family: 'Inter', -apple-system, system-ui, sans-serif;
    background: var(--bg);
    background-image: radial-gradient(ellipse at 50% 0%, rgba(59,130,246,0.08) 0%, transparent 60%);
    color: var(--text);
    min-height: 100vh;
    display: flex;
    justify-content: center;
    padding: 20px;
    user-select: none;
}
.app {
    width: 100%;
    max-width: 1200px;
    display: flex;
    flex-direction: column;
    gap: 12px;
}
.main-layout {
    display: flex;
    gap: 12px;
    align-items: flex-start;
}
.left-panel {
    width: 420px;
    flex-shrink: 0;
    position: sticky;
    top: 20px;
}
.right-panel {
    flex: 1;
    min-width: 0;
    display: flex;
    flex-direction: column;
    gap: 12px;
}

/* ── Market Tabs ── */
.market-tabs {
    display: flex;
    gap: 6px;
    padding: 10px 16px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    overflow-x: auto;
}
.market-tab {
    padding: 8px 16px;
    border-radius: 10px;
    border: 1px solid var(--border);
    background: transparent;
    color: var(--text-muted);
    font-family: 'Inter', sans-serif;
    font-size: 13px;
    font-weight: 600;
    cursor: pointer;
    white-space: nowrap;
    transition: all 0.15s;
}
.market-tab:hover { background: var(--surface-hover); color: var(--text); border-color: var(--blue); }
.market-tab.active { background: var(--blue); color: white; border-color: var(--blue); }

/* ── Header ── */
header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 16px 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
}
.logo {
    font-size: 20px;
    font-weight: 800;
    letter-spacing: -0.5px;
    background: linear-gradient(135deg, var(--blue) 0%, #818cf8 100%);
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
}
.header-info {
    display: flex;
    align-items: center;
    gap: 16px;
}
.timer {
    font-family: 'JetBrains Mono', monospace;
    font-size: 24px;
    font-weight: 700;
    color: var(--text);
    min-width: 60px;
    text-align: right;
}
.timer.urgent { color: var(--down); animation: pulse 1s ease infinite; }
@keyframes pulse { 50% { opacity: 0.6; } }
.status { display: flex; align-items: center; gap: 6px; font-size: 12px; color: var(--text-muted); }
.status-dot {
    width: 8px; height: 8px; border-radius: 50%;
    background: var(--text-muted);
    transition: background 0.3s;
}
.status.connected .status-dot { background: var(--up); box-shadow: 0 0 8px var(--up-glow); }

/* ── Price Bar ── */
.price-bar {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 14px 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    gap: 12px;
    flex-wrap: wrap;
}
.price-group { display: flex; gap: 20px; align-items: center; }
.price-item { display: flex; flex-direction: column; gap: 2px; }
.price-label { font-size: 11px; color: var(--text-muted); text-transform: uppercase; letter-spacing: 0.5px; }
.price-value {
    font-family: 'JetBrains Mono', monospace;
    font-size: 15px;
    font-weight: 600;
}
.diff {
    font-family: 'JetBrains Mono', monospace;
    font-size: 16px;
    font-weight: 700;
    padding: 4px 10px;
    border-radius: 8px;
}
.diff.positive { color: var(--up); background: var(--up-dim); }
.diff.negative { color: var(--down); background: var(--down-dim); }
.balance { color: var(--blue); }

/* ── Trading Grid ── */
.trading-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 12px;
}
.trade-card {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 16px;
    padding: 20px;
    display: flex;
    flex-direction: column;
    gap: 14px;
    transition: border-color 0.2s;
}
.trade-card:hover { border-color: rgba(148, 163, 184, 0.25); }
.card-top {
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.side-badge {
    font-size: 14px;
    font-weight: 700;
    padding: 4px 12px;
    border-radius: 20px;
}
.up-badge { background: var(--up-dim); color: var(--up); }
.down-badge { background: var(--down-dim); color: var(--down); }
.market-price {
    font-family: 'JetBrains Mono', monospace;
    font-size: 28px;
    font-weight: 800;
}
.up-card .market-price { color: var(--up); }
.down-card .market-price { color: var(--down); }
.position-info {
    display: flex;
    justify-content: space-between;
    font-size: 13px;
    color: var(--text-muted);
    padding: 8px 12px;
    background: rgba(255,255,255,0.03);
    border-radius: 8px;
}
.position-info strong {
    font-family: 'JetBrains Mono', monospace;
    color: var(--text);
}
.pos-value {
    font-family: 'JetBrains Mono', monospace;
    color: var(--blue);
}

/* ── Buttons ── */
.btn {
    border: none;
    border-radius: 12px;
    cursor: pointer;
    font-family: 'Inter', sans-serif;
    font-weight: 700;
    transition: all 0.15s ease;
    position: relative;
    overflow: hidden;
}
.btn:active { transform: scale(0.97); }
.btn:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }

.buy-btn {
    display: flex;
    align-items: center;
    justify-content: center;
    gap: 8px;
    padding: 18px 20px;
    font-size: 16px;
    color: white;
}
.up-btn {
    background: linear-gradient(135deg, var(--up) 0%, var(--up-dark) 100%);
}
.up-btn:hover:not(:disabled) {
    box-shadow: 0 6px 24px var(--up-glow);
    transform: translateY(-2px);
}
.down-btn {
    background: linear-gradient(135deg, var(--down) 0%, var(--down-dark) 100%);
}
.down-btn:hover:not(:disabled) {
    box-shadow: 0 6px 24px var(--down-glow);
    transform: translateY(-2px);
}
.btn-label { font-size: 16px; }
.btn-amount { font-family: 'JetBrains Mono', monospace; font-size: 18px; }
.btn-key {
    font-size: 11px;
    opacity: 0.6;
    background: rgba(255,255,255,0.15);
    padding: 2px 6px;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
}
.sell-row { display: flex; gap: 8px; }
.sell-btn {
    flex: 1;
    padding: 10px;
    font-size: 12px;
    color: var(--text-muted);
    background: rgba(255,255,255,0.04);
    border: 1px solid var(--border);
}
.sell-btn:hover:not(:disabled) {
    background: var(--down-dim);
    border-color: var(--down);
    color: var(--down);
}
.sell-all-btn { font-weight: 800; }

/* Loading state */
.btn.loading {
    pointer-events: none;
}
.btn.loading::after {
    content: '';
    position: absolute;
    inset: 0;
    background: rgba(0,0,0,0.3);
    display: flex;
    align-items: center;
    justify-content: center;
}
@keyframes spin { to { transform: rotate(360deg); } }

/* ── Amount Bar ── */
.amount-bar {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 12px 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
}
.amount-label {
    font-size: 13px;
    font-weight: 600;
    color: var(--text-muted);
    min-width: 36px;
}
.amount-btns { display: flex; gap: 6px; flex: 1; }
.amount-btn {
    flex: 1;
    padding: 8px 4px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    font-weight: 600;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: transparent;
    color: var(--text-muted);
    cursor: pointer;
    transition: all 0.15s;
}
.amount-btn:hover { background: var(--blue-dim); border-color: var(--blue); color: var(--text); }
.amount-btn.active {
    background: var(--blue);
    border-color: var(--blue);
    color: white;
    box-shadow: 0 2px 10px rgba(59, 130, 246, 0.3);
}
.custom-input {
    width: 80px;
    padding: 8px 10px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 13px;
    font-weight: 600;
    border: 1px solid var(--border);
    border-radius: 8px;
    background: transparent;
    color: var(--text);
    outline: none;
    transition: border-color 0.2s;
}
.custom-input:focus { border-color: var(--blue); }
.custom-input::placeholder { color: var(--text-muted); }

/* ── Limit Order Bar ── */
.limit-bar {
    display: flex;
    align-items: center;
    gap: 8px;
    padding: 12px 20px;
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
}
.limit-input {
    width: 140px;
    flex-shrink: 0;
}
.limit-btn {
    padding: 8px 12px;
    font-size: 12px;
    font-weight: 700;
    border-radius: 8px;
    white-space: nowrap;
    flex: 1;
}
.limit-buy-up { background: var(--up-dim); color: var(--up); border: 1px solid var(--up); }
.limit-buy-up:hover:not(:disabled) { background: var(--up); color: white; }
.limit-sell-up { background: transparent; color: var(--up); border: 1px solid var(--border); }
.limit-sell-up:hover:not(:disabled) { background: var(--up-dim); border-color: var(--up); }
.limit-buy-dn { background: var(--down-dim); color: var(--down); border: 1px solid var(--down); }
.limit-buy-dn:hover:not(:disabled) { background: var(--down); color: white; }
.limit-sell-dn { background: transparent; color: var(--down); border: 1px solid var(--border); }
.limit-sell-dn:hover:not(:disabled) { background: var(--down-dim); border-color: var(--down); }
.cancel-all-btn { background: transparent; color: #f59e0b; border: 1px solid #f59e0b; }
.cancel-all-btn:hover:not(:disabled) { background: #f59e0b; color: white; }

/* ── Trade Log ── */
.log-section {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    overflow: hidden;
    flex: 1;
    min-height: 120px;
    display: flex;
    flex-direction: column;
}
.log-header {
    padding: 10px 16px;
    font-size: 13px;
    font-weight: 600;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
}
.log-content {
    padding: 8px;
    overflow-y: auto;
    max-height: 200px;
    display: flex;
    flex-direction: column;
    gap: 4px;
}
.trade-entry {
    padding: 8px 12px;
    border-radius: 8px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    line-height: 1.4;
    animation: slideIn 0.25s ease;
    display: flex;
    justify-content: space-between;
    align-items: center;
}
.trade-entry.success { background: var(--up-dim); border-left: 3px solid var(--up); }
.trade-entry.error { background: var(--down-dim); border-left: 3px solid var(--down); }
.trade-ms { font-size: 11px; color: var(--text-muted); white-space: nowrap; margin-left: 8px; }
@keyframes slideIn {
    from { opacity: 0; transform: translateY(-8px); }
    to { opacity: 1; transform: translateY(0); }
}
.no-trades { color: var(--text-muted); font-size: 13px; text-align: center; padding: 20px; }

/* ── Error Banner ── */
.banner {
    padding: 12px 20px;
    border-radius: 12px;
    font-size: 13px;
    font-weight: 600;
}
.banner.error {
    background: var(--down-dim);
    border: 1px solid rgba(239, 68, 68, 0.3);
    color: var(--down);
}

/* ── Order Book ── */
.orderbook-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    overflow: hidden;
}
.ob-header {
    padding: 10px 16px;
    font-size: 13px;
    font-weight: 600;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
}
.ob-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 0;
}
.ob-side {
    padding: 8px 12px;
}
.ob-side:first-child {
    border-right: 1px solid var(--border);
}
.ob-side-label {
    font-size: 11px;
    font-weight: 700;
    text-transform: uppercase;
    letter-spacing: 0.5px;
    margin-bottom: 6px;
    display: flex;
    justify-content: space-between;
}
.ob-side-label.up-label { color: var(--up); }
.ob-side-label.dn-label { color: var(--down); }
.ob-rows {
    height: 180px;
    overflow: hidden;
}
.ob-row {
    display: flex;
    justify-content: space-between;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    line-height: 12px;
    height: 18px;
    padding: 3px 0;
    position: relative;
}
.ob-row .ob-price { z-index: 1; }
.ob-row .ob-size { z-index: 1; color: var(--text-muted); }
.ob-row .ob-bar {
    position: absolute;
    top: 0; bottom: 0;
    border-radius: 3px;
    opacity: 0.12;
}
.ob-row.bid .ob-price { color: var(--up); }
.ob-row.bid .ob-bar { right: 0; background: var(--up); }
.ob-row.ask .ob-price { color: var(--down); }
.ob-row.ask .ob-bar { left: 0; background: var(--down); }
.ob-spread {
    text-align: center;
    font-size: 11px;
    color: var(--text-muted);
    padding: 4px 0;
    border-top: 1px solid var(--border);
    font-family: 'JetBrains Mono', monospace;
}

/* ── Scrollbar ── */
::-webkit-scrollbar { width: 6px; }
::-webkit-scrollbar-track { background: transparent; }
::-webkit-scrollbar-thumb { background: var(--border); border-radius: 3px; }

/* ── Leaderboard ── */
.lb-panel {
    background: var(--surface);
    border: 1px solid var(--border);
    border-radius: 14px;
    overflow: hidden;
}
.lb-header {
    padding: 10px 16px;
    font-size: 13px;
    font-weight: 600;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
    align-items: center;
    flex-wrap: wrap;
    gap: 6px;
}
.lb-tf-tabs {
    display: flex;
    gap: 4px;
    background: rgba(255,255,255,0.04);
    border-radius: 8px;
    padding: 2px;
}
.lb-tf-tab {
    padding: 4px 12px;
    border-radius: 6px;
    border: none;
    background: transparent;
    color: var(--text-muted);
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    font-weight: 600;
    cursor: pointer;
    transition: all 0.15s;
}
.lb-tf-tab:hover { background: rgba(255,255,255,0.06); color: var(--text); }
.lb-tf-tab.active { background: var(--blue); color: white; }
.lb-stats {
    display: flex;
    gap: 12px;
    padding: 6px 16px;
    font-size: 11px;
    color: var(--text-muted);
    border-bottom: 1px solid var(--border);
    flex-wrap: wrap;
}
.lb-stats .lb-stat-tag {
    padding: 2px 8px;
    border-radius: 6px;
    background: rgba(255,255,255,0.04);
}
.lb-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
}
.lb-col {
    padding: 0;
}
.lb-col:first-child {
    border-right: 1px solid var(--border);
}
.lb-col-header {
    padding: 8px 12px;
    font-size: 12px;
    font-weight: 700;
    border-bottom: 1px solid var(--border);
    display: flex;
    justify-content: space-between;
}
.lb-col-header.up-col { color: var(--up); background: rgba(34,197,94,0.05); }
.lb-col-header.dn-col { color: var(--down); background: rgba(239,68,68,0.05); }
.lb-list {
    max-height: 600px;
    overflow-y: auto;
}
.lb-row {
    display: flex;
    align-items: center;
    padding: 3px 10px;
    font-size: 11.5px;
    font-family: 'JetBrains Mono', monospace;
    line-height: 1.6;
    border-bottom: 1px solid rgba(255,255,255,0.03);
    gap: 0;
}
.lb-row:hover { background: rgba(255,255,255,0.04); }
.lb-rank { color: var(--text-muted); width: 28px; text-align: right; flex-shrink: 0; padding-right: 6px; }
.lb-addr { color: var(--text); width: 240px; flex-shrink: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
.lb-addr a { color: inherit; text-decoration: none; }
.lb-addr a:hover { color: var(--blue); text-decoration: underline; }
.lb-name { color: var(--blue); font-size: 10px; margin-left: 2px; }
.lb-shares {
    color: #fff;
    font-weight: 700;
    font-size: 12px;
    width: 80px;
    text-align: right;
    flex-shrink: 0;
    padding: 1px 6px;
    border-radius: 4px;
    background: rgba(255,255,255,0.06);
}
.lb-pct { color: var(--text-dim); width: 48px; text-align: right; flex-shrink: 0; font-size: 10px; padding-left: 4px; }
.lb-tags { width: 50px; flex-shrink: 0; text-align: right; font-size: 14px; letter-spacing: 1px; }
.lb-pnl { font-size: 10px; width: 65px; text-align: right; flex-shrink: 0; }
.lb-pnl.positive { color: var(--up); }
.lb-pnl.negative { color: var(--down); }
.up-col .lb-shares, .lb-col:first-child .lb-shares { background: rgba(34,197,94,0.1); }
.dn-col .lb-shares, .lb-col:last-child .lb-shares { background: rgba(239,68,68,0.1); }
.lb-row.streak-row { color: var(--down); }
.lb-empty { padding: 20px; text-align: center; color: var(--text-dim); font-size: 12px; }
</style>
</head>
<body>
<div class="app">
    <!-- Error Banner -->
    <div id="error-banner" class="banner error" style="display:none">
        ⚠️ 密钥未配置 — 请在 .env 中设置 QUICK_PRIVATE_KEY 和 QUICK_FUNDER
    </div>

    <div id="status" class="status" style="display:none"><span class="status-dot"></span><span id="status-text"></span></div>

    <!-- Account Selector + Market Tabs -->
    <div class="market-tabs" id="market-tabs">
        <button class="market-tab active" data-market="btc_5m" onclick="switchMarket('btc_5m')">BTC 5m</button>
        <button class="market-tab" data-market="btc_15m" onclick="switchMarket('btc_15m')">BTC 15m</button>
        <button class="market-tab" data-market="btc_1h" onclick="switchMarket('btc_1h')">BTC 1h</button>
        <button class="market-tab" data-market="eth_5m" onclick="switchMarket('eth_5m')">ETH 5m</button>
        <button class="market-tab" data-market="eth_15m" onclick="switchMarket('eth_15m')">ETH 15m</button>
        <button class="market-tab" data-market="eth_1h" onclick="switchMarket('eth_1h')">ETH 1h</button>
        <span style="margin-left:auto; display:flex; align-items:center; gap:6px;">
            <span style="color:var(--text-dim); font-size:12px;">👛</span>
            <select id="account-select" onchange="switchAccount(this.value)"
                style="background:var(--card-bg); color:var(--text); border:1px solid var(--border);
                       border-radius:6px; padding:4px 8px; font-size:12px; cursor:pointer;
                       outline:none; font-family:inherit;">
            </select>
        </span>
    </div>

    <!-- Main two-column layout -->
    <div class="main-layout">
        <!-- Left: Order Book -->
        <div class="left-panel">
            <div class="orderbook-panel">
                <div class="ob-header">
                    <span>📊 Order Book (10档)</span>
                    <span id="ob-spread-header" style="font-family:'JetBrains Mono',monospace"></span>
                </div>
                <div class="ob-grid">
                    <div class="ob-side" id="ob-up">
                        <div class="ob-side-label up-label"><span>🟢 UP</span><span>价格 / 数量</span></div>
                        <div id="ob-up-asks" class="ob-rows"></div>
                        <div id="ob-up-spread" class="ob-spread"></div>
                        <div id="ob-up-bids" class="ob-rows"></div>
                    </div>
                    <div class="ob-side" id="ob-dn">
                        <div class="ob-side-label dn-label"><span>🔴 DOWN</span><span>价格 / 数量</span></div>
                        <div id="ob-dn-asks" class="ob-rows"></div>
                        <div id="ob-dn-spread" class="ob-spread"></div>
                        <div id="ob-dn-bids" class="ob-rows"></div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Right: Everything else -->
        <div class="right-panel">
            <!-- Price Bar -->
            <div class="price-bar">
                <div class="price-group">
                    <div class="price-item">
                        <div class="price-label">📌 锚定价</div>
                        <div id="strike" class="price-value">--</div>
                    </div>
                    <div class="price-item">
                        <div class="price-label">💰 <span id="crypto-label">BTC</span> 现价</div>
                        <div id="crypto-current" class="price-value">--</div>
                    </div>
                    <div class="price-item">
                        <div id="diff" class="diff">--</div>
                    </div>
                </div>
                <div style="display:flex; align-items:center; gap:16px">
                    <div class="price-item">
                        <div class="price-label">⏱ 倒计时</div>
                        <div id="timer" class="timer" style="font-size:18px">--:--</div>
                    </div>
                    <div class="price-item" style="text-align:right">
                        <div class="price-label">💵 pUSD 余额</div>
                        <div id="balance" class="price-value balance">--</div>
                    </div>
                </div>
            </div>

            <!-- Trading Cards -->
            <div class="trading-grid">
                <!-- UP Card -->
                <div class="trade-card up-card">
                    <div class="card-top">
                        <span class="side-badge up-badge">🟢 UP 涨</span>
                        <span id="up-price" class="market-price">--</span>
                    </div>
                    <div class="position-info">
                        <span>持仓: <strong id="up-pos">0</strong> 股</span>
                        <span id="up-value" class="pos-value"></span>
                    </div>
                    <button class="btn buy-btn up-btn" onclick="trade('buy','up')" id="btn-buy-up">
                        <span class="btn-label">BUY UP</span>
                        <span class="btn-amount buy-amount">$20</span>
                        <span class="btn-key">↑</span>
                    </button>
                    <div class="sell-row">
                        <button class="btn sell-btn" onclick="trade('sell','up')" id="btn-sell-up">
                            SELL <span class="buy-amount">$20</span>
                        </button>
                        <button class="btn sell-btn sell-all-btn" onclick="trade('sell_all','up')" id="btn-sellall-up">
                            SELL ALL <span class="btn-key">Q</span>
                        </button>
                    </div>
                </div>

                <!-- DOWN Card -->
                <div class="trade-card down-card">
                    <div class="card-top">
                        <span class="side-badge down-badge">🔴 DOWN 跌</span>
                        <span id="dn-price" class="market-price">--</span>
                    </div>
                    <div class="position-info">
                        <span>持仓: <strong id="dn-pos">0</strong> 股</span>
                        <span id="dn-value" class="pos-value"></span>
                    </div>
                    <button class="btn buy-btn down-btn" onclick="trade('buy','down')" id="btn-buy-down">
                        <span class="btn-label">BUY DOWN</span>
                        <span class="btn-amount buy-amount">$20</span>
                        <span class="btn-key">↓</span>
                    </button>
                    <div class="sell-row">
                        <button class="btn sell-btn" onclick="trade('sell','down')" id="btn-sell-down">
                            SELL <span class="buy-amount">$20</span>
                        </button>
                        <button class="btn sell-btn sell-all-btn" onclick="trade('sell_all','down')" id="btn-sellall-down">
                            SELL ALL <span class="btn-key">W</span>
                        </button>
                    </div>
                </div>
            </div>

            <!-- Amount Selector -->
            <div class="amount-bar">
                <span class="amount-label">金额</span>
                <div class="amount-btns">
                    <button class="amount-btn" data-amount="5" onclick="selectAmount(5)">$5</button>
                    <button class="amount-btn" data-amount="10" onclick="selectAmount(10)">$10</button>
                    <button class="amount-btn active" data-amount="20" onclick="selectAmount(20)">$20</button>
                    <button class="amount-btn" data-amount="50" onclick="selectAmount(50)">$50</button>
                    <button class="amount-btn" data-amount="100" onclick="selectAmount(100)">$100</button>
                </div>
                <input type="number" id="custom-amount" class="custom-input" placeholder="自定义"
                       min="1" max="10000" onchange="selectAmount(+this.value)">
            </div>

            <!-- Limit Order -->
            <div class="limit-bar">
                <span class="amount-label">限价</span>
                <input type="number" id="limit-price" class="custom-input limit-input" placeholder="输入价格 (0.01-0.99)"
                       min="0.01" max="0.99" step="0.01">
                <button class="btn limit-btn limit-buy-up" onclick="limitTrade('limit_buy','up')">挂买 UP</button>
                <button class="btn limit-btn limit-sell-up" onclick="limitTrade('limit_sell','up')">挂卖 UP</button>
                <button class="btn limit-btn limit-buy-dn" onclick="limitTrade('limit_buy','down')">挂买 DN</button>
                <button class="btn limit-btn limit-sell-dn" onclick="limitTrade('limit_sell','down')">挂卖 DN</button>
                <button class="btn limit-btn cancel-all-btn" onclick="cancelAllOrders()">撤全部</button>
            </div>

        </div>
    </div>

    <!-- Leaderboard -->
    <div class="lb-panel" id="lb-panel" style="display:none">
        <div class="lb-header">
            <span>🏆 链上持仓排行榜</span>
            <div class="lb-tf-tabs">
                <button class="lb-tf-tab active" onclick="switchLbTf('5m')" id="lb-tf-5m">5m</button>
                <button class="lb-tf-tab" onclick="switchLbTf('15m')" id="lb-tf-15m">15m</button>
                <button class="lb-tf-tab" onclick="switchLbTf('1h')" id="lb-tf-1h">1h</button>
            </div>
            <span id="lb-block" style="font-family:'JetBrains Mono',monospace; font-size:11px"></span>
        </div>
        <div class="lb-stats" id="lb-stats"></div>
        <div class="lb-grid">
            <div class="lb-col">
                <div class="lb-col-header up-col">
                    <span id="lb-up-hdr">🟢 UP</span>
                    <span id="lb-up-total"></span>
                </div>
                <div class="lb-list" id="lb-up-list"></div>
            </div>
            <div class="lb-col">
                <div class="lb-col-header dn-col">
                    <span id="lb-dn-hdr">🔴 DOWN</span>
                    <span id="lb-dn-total"></span>
                </div>
                <div class="lb-list" id="lb-dn-list"></div>
            </div>
        </div>
    </div>

    <!-- Trade Log -->
    <div class="log-section">
        <div class="log-header">📋 交易记录</div>
        <div id="trade-log" class="log-content">
            <div class="no-trades">暂无交易记录 — 按 U / D 快速下单</div>
        </div>
    </div>
</div>

<script>
/* ── 状态 ── */
let ws = null;
let selectedAmount = 20;
let reconnectTimer = null;
let lastTradeCount = 0;
let _lbTf = '5m';  // 当前选中的排行榜时间维度
let _lbDataCache = {};  // 缓存各时间维度的排行榜数据

/* ── WebSocket 连接 ── */
function connect() {
    ws = new WebSocket(`ws://${location.host}/ws`);

    ws.onopen = () => {
        document.getElementById('status').className = 'status connected';
        document.getElementById('status-text').textContent = '已连接';
    };

    ws.onmessage = (e) => {
        try { updateUI(JSON.parse(e.data)); } catch(err) {}
    };

    ws.onclose = () => {
        document.getElementById('status').className = 'status';
        document.getElementById('status-text').textContent = '断开';
        reconnectTimer = setTimeout(connect, 1000);
    };

    ws.onerror = () => ws.close();
}

/* ── UI 更新 ── */
function fmt(n, d) { return n.toLocaleString('en', {minimumFractionDigits: d, maximumFractionDigits: d}); }

function updateUI(data) {
    // 密钥检查
    if (!data.keys_ok) {
        document.getElementById('error-banner').style.display = 'block';
        document.querySelectorAll('.buy-btn, .sell-btn').forEach(b => b.disabled = true);
    }

    // 倒计时
    const remaining = Math.max(0, data.round_end - data.server_time);
    const min = Math.floor(remaining / 60);
    const sec = Math.floor(remaining % 60);
    const timerEl = document.getElementById('timer');
    timerEl.textContent = `${min}:${sec.toString().padStart(2, '0')}`;
    timerEl.className = remaining < 30 ? 'timer urgent' : 'timer';

    // 同步 tab 高亮
    if (data.active_market) {
        document.querySelectorAll('.market-tab').forEach(t => {
            t.classList.toggle('active', t.dataset.market === data.active_market);
        });
        // 更新 crypto 标签 (BTC / ETH)
        const sym = data.active_market.startsWith('eth') ? 'ETH' : 'BTC';
        document.getElementById('crypto-label').textContent = sym;
    }

    // 同步账号选择器
    const acctSel = document.getElementById('account-select');
    if (data.accounts && acctSel) {
        if (acctSel.options.length !== data.accounts.length) {
            acctSel.innerHTML = '';
            data.accounts.forEach((label, i) => {
                const opt = document.createElement('option');
                opt.value = i;
                opt.textContent = label;
                acctSel.appendChild(opt);
            });
        }
        if (acctSel.value != data.active_account) {
            acctSel.value = data.active_account;
        }
    }

    // 价格
    if (data.strike > 0) {
        document.getElementById('strike').textContent = `$${fmt(data.strike, 2)}`;
    } else {
        document.getElementById('strike').textContent = '--';
    }
    if (data.crypto_current > 0) {
        document.getElementById('crypto-current').textContent = `$${fmt(data.crypto_current, 2)}`;
    } else {
        document.getElementById('crypto-current').textContent = '--';
    }
    if (data.strike > 0 && data.crypto_current > 0) {
        const diff = data.crypto_current - data.strike;
        const diffEl = document.getElementById('diff');
        diffEl.textContent = `${diff >= 0 ? '+' : ''}$${fmt(Math.abs(diff), 2)}`;
        diffEl.className = `diff ${diff >= 0 ? 'positive' : 'negative'}`;
    } else {
        document.getElementById('diff').textContent = '--';
        document.getElementById('diff').className = 'diff';
    }

    // USDC 余额
    if (data.usdc_balance > 0) {
        document.getElementById('balance').textContent = `$${fmt(data.usdc_balance, 2)}`;
    }

    // UP 盘口 + 持仓
    const upP = data.up_price;
    document.getElementById('up-price').textContent = upP > 0 ? `${Math.round(upP * 100)}¢` : '--';
    const upPos = data.up_position;
    document.getElementById('up-pos').textContent = upPos > 0 ? fmt(upPos, 1) : '0';
    document.getElementById('up-value').textContent = upPos > 0 && upP > 0 ? `≈$${fmt(upPos * upP, 2)}` : '';

    // DOWN 盘口 + 持仓
    const dnP = data.down_price;
    document.getElementById('dn-price').textContent = dnP > 0 ? `${Math.round(dnP * 100)}¢` : '--';
    const dnPos = data.down_position;
    document.getElementById('dn-pos').textContent = dnPos > 0 ? fmt(dnPos, 1) : '0';
    document.getElementById('dn-value').textContent = dnPos > 0 && dnP > 0 ? `≈$${fmt(dnPos * dnP, 2)}` : '';

    // Order Book
    renderOrderBook('up', data.up_book);
    renderOrderBook('dn', data.down_book);

    // Leaderboard
    if (data.leaderboard) {
        _lbDataCache['5m'] = data.leaderboard;
    }
    if (data.leaderboard_15m) {
        _lbDataCache['15m'] = data.leaderboard_15m;
    }
    if (data.leaderboard_1h) {
        _lbDataCache['1h'] = data.leaderboard_1h;
    }
    if (_lbDataCache[_lbTf]) {
        renderLeaderboard(_lbDataCache[_lbTf]);
    }

    // 交易记录 (仅在有新记录时更新)
    const trades = data.trades || [];
    if (trades.length !== lastTradeCount) {
        lastTradeCount = trades.length;
        renderTrades(trades);
    }
}

function renderOrderBook(side, book) {
    if (!book) return;
    const bids = book.bids || [];
    const asks = book.asks || [];
    const prefix = side === 'up' ? 'ob-up' : 'ob-dn';
    const OB_LEVELS = 10;
    // Max size for bar width
    const allSizes = [...bids, ...asks].map(l => l.s);
    const maxSize = Math.max(...allSizes, 1);

    const emptyRow = '<div class="ob-row"><span class="ob-price">&nbsp;</span><span class="ob-size">&nbsp;</span></div>';

    // Asks (reversed: highest at top)
    const asksReversed = [...asks].reverse();
    const askRows = asksReversed.map(a => {
        const pct = (a.s / maxSize * 100).toFixed(0);
        return `<div class="ob-row ask">
            <span class="ob-bar" style="width:${pct}%"></span>
            <span class="ob-price">${(a.p * 100).toFixed(0)}¢</span>
            <span class="ob-size">${fmt(a.s, 0)}</span>
        </div>`;
    });
    while (askRows.length < OB_LEVELS) askRows.unshift(emptyRow);
    document.getElementById(`${prefix}-asks`).innerHTML = askRows.join('');

    // Spread
    const bestBid = bids.length ? bids[0].p : 0;
    const bestAsk = asks.length ? asks[0].p : 0;
    const spread = bestAsk > 0 && bestBid > 0 ? ((bestAsk - bestBid) * 100).toFixed(0) : '--';
    document.getElementById(`${prefix}-spread`).textContent = `spread ${spread}¢`;

    // Bids
    const bidRows = bids.map(b => {
        const pct = (b.s / maxSize * 100).toFixed(0);
        return `<div class="ob-row bid">
            <span class="ob-bar" style="width:${pct}%"></span>
            <span class="ob-price">${(b.p * 100).toFixed(0)}¢</span>
            <span class="ob-size">${fmt(b.s, 0)}</span>
        </div>`;
    });
    while (bidRows.length < OB_LEVELS) bidRows.push(emptyRow);
    document.getElementById(`${prefix}-bids`).innerHTML = bidRows.join('');
}

function renderTrades(trades) {
    const log = document.getElementById('trade-log');
    if (!trades.length) {
        log.innerHTML = '<div class="no-trades">暂无交易记录 — 按 U / D 快速下单</div>';
        return;
    }
    log.innerHTML = trades.map(t => {
        const icon = t.success ? '✅' : '❌';
        const cls = t.success ? 'success' : 'error';
        const ms = t.elapsed_ms ? `${t.elapsed_ms}ms` : '';
        return `<div class="trade-entry ${cls}">
            <span>${t.time}  ${icon} ${t.message}</span>
            <span class="trade-ms">${ms}</span>
        </div>`;
    }).join('');
}

/* ── 交易 ── */
async function trade(action, side) {
    // 找到被点击的按钮 (或键盘触发的按钮)
    const btnId = action === 'sell_all' ? `btn-sellall-${side}`
                : action === 'sell' ? `btn-sell-${side}`
                : `btn-buy-${side}`;
    const btn = document.getElementById(btnId);

    const amount = action === 'sell_all' ? 'all' : selectedAmount;

    if (btn) { btn.disabled = true; btn.classList.add('loading'); }

    try {
        const resp = await fetch('/api/trade', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({action, side, amount}),
        });
        const result = await resp.json();

        // 闪烁反馈
        if (btn) {
            btn.style.boxShadow = result.success
                ? '0 0 30px var(--up-glow)' : '0 0 30px var(--down-glow)';
            setTimeout(() => btn.style.boxShadow = '', 500);
        }
    } catch (e) {
        // 网络错误
    } finally {
        if (btn) { btn.disabled = false; btn.classList.remove('loading'); }
    }
}

/* ── 金额选择 ── */
function selectAmount(amt) {
    if (!amt || amt <= 0) return;
    selectedAmount = amt;

    // 更新按钮样式
    document.querySelectorAll('.amount-btn').forEach(b => {
        b.classList.toggle('active', +b.dataset.amount === amt);
    });

    // 更新所有按钮上的金额显示
    document.querySelectorAll('.buy-amount').forEach(el => {
        el.textContent = `$${amt}`;
    });

    // 清空自定义输入 (如果选了预设)
    const customInput = document.getElementById('custom-amount');
    if ([5, 10, 20, 50, 100].includes(amt)) {
        customInput.value = '';
    }
}

/* ── 切换市场 ── */
async function switchMarket(mktKey) {
    // 立即更新 tab UI
    document.querySelectorAll('.market-tab').forEach(t => {
        t.classList.toggle('active', t.dataset.market === mktKey);
    });
    // 清空显示
    document.getElementById('strike').textContent = '--';
    document.getElementById('crypto-current').textContent = '--';
    document.getElementById('diff').textContent = '--';
    document.getElementById('up-price').textContent = '--';
    document.getElementById('dn-price').textContent = '--';
    try {
        await fetch('/api/switch_market', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({market: mktKey}),
        });
    } catch (e) {}
}

/* ── 切换账号 ── */
async function switchAccount(idx) {
    try {
        await fetch('/api/switch_account', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({index: parseInt(idx)}),
        });
    } catch (e) {}
}

/* ── 撤单 ── */
async function cancelAllOrders() {
    try {
        const resp = await fetch('/api/trade', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({action: 'cancel_all_orders', side: 'up'}),
        });
        const result = await resp.json();
    } catch (e) {}
}

/* ── 限价单 ── */
async function limitTrade(action, side) {
    const priceInput = document.getElementById('limit-price');
    const price = parseFloat(priceInput.value);
    if (!price || price <= 0 || price >= 1) {
        priceInput.style.borderColor = 'var(--down)';
        priceInput.focus();
        setTimeout(() => priceInput.style.borderColor = '', 1000);
        return;
    }
    try {
        const resp = await fetch('/api/trade', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({action, side, amount: selectedAmount, price}),
        });
        const result = await resp.json();
        // 闪烁反馈
        priceInput.style.borderColor = result.success ? 'var(--up)' : 'var(--down)';
        setTimeout(() => priceInput.style.borderColor = '', 1000);
    } catch (e) {
        priceInput.style.borderColor = 'var(--down)';
        setTimeout(() => priceInput.style.borderColor = '', 1000);
    }
}

/* ── 键盘快捷键 ── */
document.addEventListener('keydown', (e) => {
    // 忽略输入框中的按键
    if (e.target.tagName === 'INPUT') return;

    const key = e.key;
    switch (key) {
        case 'ArrowUp': e.preventDefault(); trade('buy', 'up'); break;
        case 'ArrowDown': e.preventDefault(); trade('buy', 'down'); break;
        case 'q': trade('sell_all', 'up'); break;
        case 'w': trade('sell_all', 'down'); break;
        case '1': selectAmount(5); break;
        case '2': selectAmount(10); break;
        case '3': selectAmount(20); break;
        case '4': selectAmount(50); break;
        case '5': selectAmount(100); break;
    }
});

/* ── Leaderboard 渲染 ── */
function renderLeaderboard(lb) {
    const panel = document.getElementById('lb-panel');
    panel.style.display = '';

    // Header
    document.getElementById('lb-block').textContent = `区块 ${lb.scan_block || ''}`;

    // Stats bar
    let statsHtml = '';
    if (lb.hedged_count > 0) {
        statsHtml += `<span class="lb-stat-tag">⚖️ 做市商 ${lb.hedged_count}人</span>`;
    }
    if (lb.winners) {
        const w = lb.winners;
        const net = Math.abs(w.up_s - w.dn_s);
        const dir = w.up_s > w.dn_s ? 'UP' : w.dn_s > w.up_s ? 'DN' : '--';
        const arrow = dir === 'UP' ? '🟢' : dir === 'DN' ? '🔴' : '⚪';
        statsHtml += `<span class="lb-stat-tag">🏅 上轮${w.side.toUpperCase()}赢家 ${w.total}人 | 本轮: ${w.up_n}人UP(${w.up_s.toLocaleString()}股) vs ${w.dn_n}人DN(${w.dn_s.toLocaleString()}股) | ${arrow} 净${dir} ${net.toLocaleString()}股</span>`;
    }
    if (lb.streak4) {
        const s4 = lb.streak4;
        const s4arrow = s4.up > s4.dn ? '🟢' : s4.dn > s4.up ? '🔴' : '⚪';
        statsHtml += `<span class="lb-stat-tag">📊 4连胜 ${s4.total}人 | ${s4.up}人UP vs ${s4.dn}人DN ${s4arrow}</span>`;
    }
    document.getElementById('lb-stats').innerHTML = statsHtml;

    // Column headers
    const sUp = lb.streak_up ? ` 🔥${lb.streak_up}` : '';
    const sDn = lb.streak_dn ? ` 🔥${lb.streak_dn}` : '';
    document.getElementById('lb-up-hdr').textContent = `🟢 UP (${lb.up_count}人)${sUp}`;
    document.getElementById('lb-up-total').textContent = `${lb.up_total.toLocaleString()}股`;
    document.getElementById('lb-dn-hdr').textContent = `🔴 DOWN (${lb.dn_count}人)${sDn}`;
    document.getElementById('lb-dn-total').textContent = `${lb.dn_total.toLocaleString()}股`;

    // Render rows
    document.getElementById('lb-up-list').innerHTML = renderLbCol(lb.up, lb.up_total);
    document.getElementById('lb-dn-list').innerHTML = renderLbCol(lb.down, lb.dn_total);
}

function renderLbCol(entries, total) {
    if (!entries || !entries.length) return '<div class="lb-empty">暂无数据</div>';
    return entries.map((e, i) => {
        const pct = total > 0 ? (e.shares / total * 100).toFixed(1) : '0.0';
        const short = e.addr.slice(0, 6) + '…' + e.addr.slice(-4);
        const tags = (e.whale ? '🐋' : '') + (e.hedged ? '⚖️' : '') + (e.streak ? '🔥' : '');
        const nameHtml = e.name ? `<span class="lb-name">(${e.name})</span>` : '';
        let pnlHtml = '';
        if (e.pnl != null) {
            const cls = e.pnl > 0 ? 'positive' : e.pnl < 0 ? 'negative' : '';
            pnlHtml = `<span class="lb-pnl ${cls}">$${e.pnl > 0 ? '+' : ''}${Math.round(e.pnl).toLocaleString()}</span>`;
        }
        const rowCls = e.streak ? 'lb-row streak-row' : 'lb-row';
        return `<div class="${rowCls}">
            <span class="lb-rank">${i+1}.</span>
            <span class="lb-addr"><a href="https://polymarket.com/profile/${e.addr}" target="_blank" title="${e.addr}">${short}</a>${nameHtml}</span>
            <span class="lb-shares">${e.shares.toLocaleString()}股</span>
            <span class="lb-pct">${pct}%</span>
            ${pnlHtml}
            <span class="lb-tags">${tags}</span>
        </div>`;
    }).join('');
}

function switchLbTf(tf) {
    _lbTf = tf;
    document.querySelectorAll('.lb-tf-tab').forEach(t => t.classList.remove('active'));
    document.getElementById('lb-tf-' + tf).classList.add('active');
    if (_lbDataCache[tf]) {
        renderLeaderboard(_lbDataCache[tf]);
    }
}

/* ── 启动 ── */
connect();
</script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════════════════════
# 主入口
# ══════════════════════════════════════════════════════════════════════════════

def _start_workers():
    """启动后台工作线程"""
    threading.Thread(target=state_worker, daemon=True).start()
    threading.Thread(target=clob_price_worker, daemon=True).start()
    if ACCOUNTS:
        def _init():
            try:
                get_clob_client()
            except Exception as e:
                print(f"  ❌ CLOB 初始化失败: {e}")
        threading.Thread(target=_init, daemon=True).start()


def start_embedded():
    """嵌入模式: 由 onchain_leaderboard 调用, 在后台线程启动 web 服务"""
    global _embedded
    _embedded = True
    _start_workers()

    async def _run_server():
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", PORT)
        await site.start()
        print(f"  ⚡ Quick Trade: http://localhost:{PORT}")
        # keep running forever
        while True:
            await asyncio.sleep(3600)

    def _thread():
        asyncio.run(_run_server())

    threading.Thread(target=_thread, daemon=True).start()


def main():
    print("=" * 55)
    print("  ⚡ Quick Trade Panel (多市场/多账号)")
    print("=" * 55)

    if not ACCOUNTS:
        print("  ⚠️  QUICK_PRIVATE_KEY / QUICK_FUNDER 未配置")
        print("     请在 .env 中添加后重启")
        print()
    else:
        for i, a in enumerate(ACCOUNTS):
            tag = " ← 当前" if i == 0 else ""
            print(f"  👛 账号{i+1}: {a['label']}{tag}")
        print()

    _start_workers()

    # 延迟打开浏览器
    threading.Timer(1.5, lambda: webbrowser.open(f"http://localhost:{PORT}")).start()

    print(f"  🌐 http://localhost:{PORT}")
    print(f"  快捷键: ↑=买UP  ↓=买DOWN  Q=卖UP  W=卖DOWN")
    print(f"  金额键: 1=$5  2=$10  3=$20  4=$50  5=$100")
    print()

    web.run_app(app, port=PORT, print=None)


if __name__ == "__main__":
    main()
