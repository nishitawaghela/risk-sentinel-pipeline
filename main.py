from fastapi import FastAPI

app = FastAPI()
 # This is a simple API endpoint that returns a JSON response indicating that the Sentinel service is online (GET request to the root URL "/"). 
def read_root():
    return {"status": "Sentinel is Online", "key": "Security"}

#POST request to "/trades/" that accepts a JSON payload representing a trade. The payload must match the structure defined by the Trade class. If the incoming data is valid according to the Trade schema, the create_trade function will return a JSON response confirming that the trade was received along with the data that was sent.

from pydantic import BaseModel # Import the "Data Validator"

# 1. Define the "Order Form" (Schema)
# This tells the Waiter EXACTLY what a valid trade looks like.
class Trade(BaseModel):
    trade_id: str
    stock_ticker: str
    price: float
    quantity: int

# 2. Create the POST Door
@app.post("/trades/")
def create_trade(trade: Trade):
    # This function only runs if the data matches the structure above.
    return {"message": "Trade received", "data": trade}