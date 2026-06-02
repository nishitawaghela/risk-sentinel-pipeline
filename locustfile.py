from locust import HttpUser, task, between
import random

# --- CONFIGURE YOUR AUTHENTICATION HERE ---
# This must perfectly match the UBS_API_KEY in your .env file
API_KEY = "ubs-secure-token-123" 

class TradeSimulator(HttpUser):
    # Sends requests as fast as possible to maximize throughput testing
    # Wait between 100ms and 500ms between trades
    wait_time = between(0.1, 0.5) 

    @task
    def send_trade(self):
        # 1. Bypass the Security Gate
        headers = {
            "X-UBS-Client-Key": API_KEY
        }
        
        # 2. Match the exact Pydantic Schema and Business Logic
        # - Max price (500) * Max quantity (5000) = 2,500,000 (Safely below the 10,000,000 limit)
        # - Avoiding restricted tickers (UBS, SCAM_CO)
        payload = {
            "trade_id": f"TRD-{random.randint(100000, 999999)}",
            "stock_ticker": random.choice(["AAPL", "TSLA", "NVDA", "MSFT", "GOOG"]),
            "price": round(random.uniform(10.0, 500.0), 2),
            "quantity": random.randint(10, 5000),
            "trader_id": "USR-LOAD-TEST"
        }
        
        # 3. Hit the exact route definition
        self.client.post("/trades/", json=payload, headers=headers)