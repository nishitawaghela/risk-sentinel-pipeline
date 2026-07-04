from locust import HttpUser, task, between
import random

# --- CONFIGURE YOUR AUTHENTICATION HERE ---
# This must perfectly match the UBS_API_KEY in your .env file
API_KEY = "ubs-secure-token-123" 

class TradeSimulator(HttpUser):
    wait_time = between(0, 0.05) 

    @task
    def send_trade(self):
        headers = {
            "X-UBS-Client-Key": API_KEY
        }
        
        # Match the exact Pydantic Schema and Business Logic (main.py's Trade model)
        # - Max price (500) * Max volume (5000) = 2,500,000 (Safely below the 10,000,000 limit)
        # - Avoiding restricted tickers (UBS, SCAM_CO)
        price = round(random.uniform(10.0, 500.0), 2)
        volume = random.randint(10, 500)
        payload = {
            "trade_id": f"TRD-{random.randint(100000, 999999)}",
            "user_id": "USR-LOAD-TEST",
            "stock_symbol": random.choice(["AAPL", "TSLA", "NVDA", "MSFT", "GOOG"]),
            "price": price,
            "volume": volume,
            "action": random.choice(["BUY", "SELL"]),
            "order_type": random.choice(["MARKET", "LIMIT"])
        }
        
        self.client.post("/trades/", json=payload, headers=headers)