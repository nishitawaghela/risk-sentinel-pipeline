import json
import random
import uuid
from datetime import datetime, timedelta

# --- CONFIGURATION ---
SYMBOLS = ["AAPL", "MSFT", "NVDA", "TSLA"]
TRADER_IDS = [f"TRD_{i:04d}" for i in range(1, 101)] # 100 normal traders
START_TIME = datetime.now()

def create_payload(timestamp, trader, symbol, price, vol, action, order_type, status, label):
    return {
        "trade_id": str(uuid.uuid4()),
        "timestamp": timestamp.isoformat(),
        "trader_id": trader,
        "symbol": symbol,
        "price": round(price, 2),
        "volume": vol,
        "action": action,
        "order_type": order_type,
        "status": status,
        "label": label
    }

def generate_normal_trade(timestamp):
    # Standard market noise
    return [create_payload(
        timestamp, random.choice(TRADER_IDS), random.choice(SYMBOLS), 
        random.uniform(100, 500), random.randint(10, 500), 
        random.choice(["BUY", "SELL"]), "MARKET", "FILLED", 0 # Label 0: Normal
    )], timestamp + timedelta(milliseconds=random.randint(100, 1000))

def inject_wash_trade(timestamp, symbol, price):
    # Label 1: Wash Trade (Rapid BUY/SELL from same user to fake volume)
    bad_actor = random.choice(TRADER_IDS)
    trades = []
    for _ in range(4): 
        for action in ["BUY", "SELL"]:
            trades.append(create_payload(timestamp, bad_actor, symbol, price, random.randint(1000, 5000), action, "MARKET", "FILLED", 1))
            timestamp += timedelta(milliseconds=random.randint(1, 5))
    return trades, timestamp

def inject_volume_spike(timestamp, symbol, price):
    # Label 2: Volume Spike (Pump and Dump simulation)
    trades = []
    for _ in range(15): 
        trades.append(create_payload(timestamp, random.choice(TRADER_IDS), symbol, price + random.uniform(0, 2), random.randint(5000, 20000), "BUY", "MARKET", "FILLED", 2))
        timestamp += timedelta(milliseconds=random.randint(10, 50))
    return trades, timestamp

def inject_layering_spoofing(timestamp, symbol, price):
    # Label 3: Layering/Spoofing (Massive cancelled limit orders to fake sell wall, then buying cheap)
    bad_actor = random.choice(TRADER_IDS)
    trades = []
    # 1. Place fake massive SELL orders
    for _ in range(5):
        trades.append(create_payload(timestamp, bad_actor, symbol, price + 1.0, random.randint(10000, 50000), "SELL", "LIMIT", "CANCELLED", 3))
        timestamp += timedelta(milliseconds=2)
    # 2. Exploit the artificially lowered price with a real BUY order
    trades.append(create_payload(timestamp, bad_actor, symbol, price - 0.5, random.randint(1000, 5000), "BUY", "MARKET", "FILLED", 3))
    return trades, timestamp

def inject_front_running(timestamp, symbol, price):
    # Label 4: Front-Running (Small buys before a known massive institutional block trade, then selling)
    bad_actor = random.choice(TRADER_IDS)
    whale = random.choice([t for t in TRADER_IDS if t != bad_actor])
    trades = []
    # 1. Bad actor buys in small chunks
    for _ in range(3):
        trades.append(create_payload(timestamp, bad_actor, symbol, price, random.randint(100, 500), "BUY", "MARKET", "FILLED", 4))
        timestamp += timedelta(milliseconds=5)
    # 2. Whale drops massive block trade (driving price up)
    trades.append(create_payload(timestamp, whale, symbol, price + 2.0, random.randint(50000, 100000), "BUY", "MARKET", "FILLED", 0)) # Whale is normal
    timestamp += timedelta(milliseconds=10)
    # 3. Bad actor dumps for profit
    trades.append(create_payload(timestamp, bad_actor, symbol, price + 2.0, random.randint(300, 1500), "SELL", "MARKET", "FILLED", 4))
    return trades, timestamp

def inject_flash_crash(timestamp, symbol, price):
    # Label 5: Flash Crash (Cascading algorithmic sell-off driving price down rapidly)
    trades = []
    current_price = price
    for _ in range(20):
        current_price *= 0.995 # Price drops 0.5% every millisecond
        trades.append(create_payload(timestamp, random.choice(TRADER_IDS), symbol, current_price, random.randint(1000, 5000), "SELL", "MARKET", "FILLED", 5))
        timestamp += timedelta(milliseconds=1)
    return trades, timestamp

def build_advanced_dataset(num_records=5000):
    dataset = []
    current_time = START_TIME
    print("Initializing Advanced Sentinel Engine Data Generation...")
    
    while len(dataset) < num_records:
        scenario = random.random()
        target_symbol = random.choice(SYMBOLS)
        target_price = round(random.uniform(100, 500), 2)
        
        # 75% Normal, 5% for each of the 5 anomalies
        if scenario < 0.75:
            trades, current_time = generate_normal_trade(current_time)
        elif scenario < 0.80:
            trades, current_time = inject_wash_trade(current_time, target_symbol, target_price)
        elif scenario < 0.85:
            trades, current_time = inject_volume_spike(current_time, target_symbol, target_price)
        elif scenario < 0.90:
            trades, current_time = inject_layering_spoofing(current_time, target_symbol, target_price)
        elif scenario < 0.95:
            trades, current_time = inject_front_running(current_time, target_symbol, target_price)
        else:
            trades, current_time = inject_flash_crash(current_time, target_symbol, target_price)
            
        dataset.extend(trades)

    # Make sure to create a 'raw' folder inside your 'data' folder first!
    with open('data/raw/advanced_trades_dataset.json', 'w') as f:
        json.dump(dataset, f, indent=4)
        
    print(f"Dataset generated successfully! Total records: {len(dataset)}")

if __name__ == "__main__":
    build_advanced_dataset(5000)