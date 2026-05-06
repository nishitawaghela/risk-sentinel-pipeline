from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from confluent_kafka import Producer
import json

# 1. Initialize the API
app = FastAPI(title="Sentinel Risk Engine - Ingestion API")

# 2. Configure the Kafka Producer to talk to our Docker container
conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'sentinel-fastapi-producer'
}
producer = Producer(conf)

# 3. Define the blueprint of a Trade using Pydantic
class Trade(BaseModel):
    trade_id: str
    user_id: str
    stock_symbol: str
    price: float
    volume: int
    action: str
    order_type: str

# 4. Create the ingestion endpoint
@app.post("/ingest")
async def ingest_trade(trade: Trade):
    try:
        # Convert the Pydantic trade model to a JSON string
        trade_json = json.dumps(trade.dict())
        
        # Fire it into the 'live_trades' Kafka topic
        producer.produce(
            topic='live_trades', 
            value=trade_json,
            key=trade.trade_id # Partitioning by trade_id keeps things ordered
        )
        
        # Flush tells Kafka to send the data IMMEDIATELY
        producer.flush()
        
        return {"status": "success", "message": f"Trade {trade.trade_id} published to Kafka stream."}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))