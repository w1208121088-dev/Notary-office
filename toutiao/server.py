import asyncio
import json
import random
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles

app = FastAPI()

# ──────────────────────────────────────────────
# 初始数据
# ──────────────────────────────────────────────

STONES_BASE = {
    "蓝水（水合二氧化铱）": 70000,
    "金": 21000,
    "银": 3500,
    "镍": 9000,
    "钨": 8500,
    "钴": 7800,
    "钛": 6200,
    "铬": 2700,
    "铜": 2200,
    "铝": 1800,
    "锌": 2500,
    "锡": 3000,
    "钼": 2600,
    "稀土磁材": 6500,
    "碳纤维": 4700,
    "凯夫拉": 4300,
    "铍铜": 5200,
    "因瓦合金": 5600,
    "哈氏合金": 7800,
    "因科镍合金": 7200,
    "超高强钢": 5100,
    "有色水晶": 1700,
}

RABBITS_BASE = {
    "雪怪兔":   {"price": 950,  "desc": "过于夸张的大兔脚。"},
    "圣花兔":   {"price": 777,  "desc": "缝纫机头缠绕着荆棘的兔子。"},
    "三角洲兔": {"price": 771,  "desc": "背上有A字红色标记的网格毛色兔子。"},
    "海豹兔":   {"price": 816,  "desc": "有大胡子和纹身的兔子。"},
    "\"水\"兔": {"price": 848,  "desc": "全身都像泡在低饱和像素中的兔子。"},
    "素子兔":   {"price": 899,  "desc": "全身机械的兔子，能像祈臣一样隐身。"},
    "火锅兔":   {"price": 500,  "desc": "毛色像火锅一般。"},
    "烩面兔":   {"price": 377,  "desc": "毛色长短不均，像白汤中的面条。"},
    "生煎兔":   {"price": 634,  "desc": "太过哈韩，有高丽旗纹身的兔子。"},
    "主任兔":   {"price": 400,  "desc": "脾气暴躁的兔子，爱国之心毋庸置疑。"},
    "花旗兔":   {"price": 177,  "desc": "不要对它说俄语。"},
    "太子兔":   {"price": 266,  "desc": "全身被阴影覆盖，只能看到剪影的神秘兔子。"},
    "千禧年":   {"price": 200,  "desc": "头部有个电子屏，显示hello world。"},
    "法官兔":   {"price": 333,  "desc": "赵志先生。"},
    "清华兔":   {"price": 348,  "desc": "穿着中山装的兔子。"},
}

STARTING_CASH = 100000

# ──────────────────────────────────────────────
# 运行时状态
# ──────────────────────────────────────────────

stone_prices  = {k: float(v) for k, v in STONES_BASE.items()}
rabbit_prices = {k: float(v["price"]) for k, v in RABBITS_BASE.items()}

players:     dict[str, dict]      = {}
connections: dict[str, WebSocket] = {}

# ──────────────────────────────────────────────
# 价格波动逻辑
# ──────────────────────────────────────────────

def apply_price_impact(prices: dict, item: str, qty: float, is_buy: bool):
    base   = (STONES_BASE.get(item) or RABBITS_BASE.get(item, {}).get("price", 1))
    impact = min(qty / base * 0.8, 0.15)
    factor = 1 + impact if is_buy else 1 - impact
    prices[item] = round(max(base * 0.2, min(base * 5.0, prices[item] * factor)), 2)

def random_drift():
    """每10秒小幅随机游走。"""
    for prices, base_dict in [
        (stone_prices,  STONES_BASE),
        (rabbit_prices, {k: v["price"] for k, v in RABBITS_BASE.items()}),
    ]:
        for item in prices:
            drift = random.uniform(-0.02, 0.02)
            base  = base_dict[item]
            prices[item] = round(max(base * 0.2, min(base * 5.0, prices[item] * (1 + drift))), 2)

async def market_event():
    """每2小时：随机5件商品大幅涨跌 10%~40%，并广播公告。"""
    all_items = (
        [(stone_prices,  "stones",  k) for k in stone_prices] +
        [(rabbit_prices, "rabbits", k) for k in rabbit_prices]
    )
    chosen = random.sample(all_items, min(5, len(all_items)))
    summaries = []
    for prices, _cat, item in chosen:
        direction = random.choice([1, -1])
        magnitude = random.uniform(0.10, 0.40)
        base      = (STONES_BASE.get(item) or RABBITS_BASE.get(item, {}).get("price", 1))
        prices[item] = round(max(base * 0.2, min(base * 5.0, prices[item] * (1 + direction * magnitude))), 2)
        arrow = "↑" if direction == 1 else "↓"
        summaries.append(f"{item} {arrow}{int(magnitude * 100)}%")
    pass  # 悄悄调整，不通知玩家

# ──────────────────────────────────────────────
# 广播与推送
# ──────────────────────────────────────────────

def market_snapshot():
    return {"type": "market", "stones": stone_prices, "rabbits": rabbit_prices}

async def broadcast_market():
    msg  = json.dumps(market_snapshot())
    dead = []
    for pid, ws in connections.items():
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(pid)
    for pid in dead:
        connections.pop(pid, None)

async def broadcast_event(text: str):
    msg = json.dumps({"type": "event", "msg": text})
    for ws in list(connections.values()):
        try:
            await ws.send_text(msg)
        except Exception:
            pass

async def send_player(player_id: str, data: dict):
    ws = connections.get(player_id)
    if ws:
        try:
            await ws.send_text(json.dumps(data))
        except Exception:
            pass

# ──────────────────────────────────────────────
# 后台任务：漂移 + 定时事件
# ──────────────────────────────────────────────

async def drift_loop():
    last_event = asyncio.get_event_loop().time()
    while True:
        await asyncio.sleep(10)
        random_drift()
        # 每 7200 秒（2小时）触发市场异动
        now = asyncio.get_event_loop().time()
        if now - last_event >= 7200:
            last_event = now
            await market_event()
        await broadcast_market()

@app.on_event("startup")
async def startup():
    asyncio.create_task(drift_loop())

# ──────────────────────────────────────────────
# WebSocket 主逻辑
# ──────────────────────────────────────────────

@app.websocket("/ws/{player_id}")
async def websocket_endpoint(websocket: WebSocket, player_id: str):
    await websocket.accept()
    connections[player_id] = websocket

    if player_id not in players:
        players[player_id] = {
            "cash":    STARTING_CASH,
            "stones":  {k: 0 for k in stone_prices},
            "rabbits": {k: 0 for k in rabbit_prices},
        }

    await send_player(player_id, market_snapshot())
    await send_player(player_id, {"type": "player", "data": players[player_id]})

    try:
        while True:
            raw    = await websocket.receive_text()
            msg    = json.loads(raw)
            action = msg.get("action")
            if action == "buy":
                await handle_trade(player_id, msg, is_buy=True)
            elif action == "sell":
                await handle_trade(player_id, msg, is_buy=False)
    except WebSocketDisconnect:
        connections.pop(player_id, None)

# ──────────────────────────────────────────────
# 交易处理
# ──────────────────────────────────────────────

async def handle_trade(player_id: str, msg: dict, is_buy: bool):
    category = msg.get("category")
    item     = msg.get("item")
    qty      = float(msg.get("qty", 0))

    if qty <= 0:
        await send_player(player_id, {"type": "error", "msg": "数量必须大于0"})
        return

    player = players[player_id]

    if category == "stones":
        prices      = stone_prices
        holding_key = "stones"
    elif category == "rabbits":
        prices      = rabbit_prices
        holding_key = "rabbits"
        qty         = int(qty)
    else:
        await send_player(player_id, {"type": "error", "msg": "未知品类"})
        return

    if item not in prices:
        await send_player(player_id, {"type": "error", "msg": "未知商品"})
        return

    price = prices[item]
    total = price * qty

    if is_buy:
        if player["cash"] < total:
            await send_player(player_id, {"type": "error", "msg": f"钞不够，需要 {total:.0f} 钞，你只有 {player['cash']:.0f} 钞"})
            return
        player["cash"] -= total
        player[holding_key][item] = player[holding_key].get(item, 0) + qty
        action_word = "买入"
    else:
        holding = player[holding_key].get(item, 0)
        if holding < qty:
            await send_player(player_id, {"type": "error", "msg": f"持仓不足，你只有 {holding}"})
            return
        player["cash"] += total
        player[holding_key][item] = holding - qty
        action_word = "卖出"

    apply_price_impact(prices, item, qty, is_buy)

    await send_player(player_id, {
        "type": "trade_ok",
        "msg":  f"{action_word} {item} ×{qty}，成交价 {price:.0f} 钞/单位，合计 {total:.0f} 钞",
        "data": players[player_id],
    })
    await broadcast_market()

# ──────────────────────────────────────────────
# 静态文件
# ──────────────────────────────────────────────

app.mount("/", StaticFiles(directory="static", html=True), name="static")
