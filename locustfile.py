from locust import HttpUser, task, between
import random

class TradeSimulator(HttpUser):
    # Sends requests as fast as possible (no wait time between them)
    wait_time = between(0, 0) 

    @task
    def send_trade(self):
        payload = {
            "trade_id": f"TRD-{random.randint(1000, 99999)}",
            "user_id": "USR-TEST",
            "stock_symbol": random.choice(["AAPL", "TSLA", "NVDA"]),
            "price": random.uniform(10.0, 500.0),
            "volume": random.randint(10, 10000),
            "action": random.choice(["BUY", "SELL"]),
            "order_type": random.choice(["MARKET", "LIMIT"])
        }
        self.client.post("/trades", json=payload)