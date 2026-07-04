from fastapi import FastAPI, HTTPException, Security, status, Depends
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session
import os
import secrets
from dotenv import load_dotenv
from pydantic import BaseModel, Field 
import models
from database import SessionLocal, engine
import json
from confluent_kafka import Producer
import time

models.Base.metadata.create_all(bind=engine)

load_dotenv()
API_KEY_SECRET = os.getenv("UBS_API_KEY")
API_KEY_NAME = "X-UBS-Client-Key"

if not API_KEY_SECRET:
    raise RuntimeError("CRITICAL ERROR: UBS_API_KEY not found in environment!")

app = FastAPI(title="UBS Sentinel: Trade Ingestion API")
kafka_producer = Producer({'bootstrap.servers': 'localhost:9092'})

def get_db():
    db = SessionLocal()
    try:
        yield db
    except BaseException:
        db.rollback()
        raise
    finally:
        db.close()

api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    if api_key_header and secrets.compare_digest(api_key_header, API_KEY_SECRET):
        return api_key_header
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials (Access Denied)"
        )

RESTRICTED_STOCKS = {"UBS", "UBSG", "SCAM_CO", "BLOCKED_LTD"}
SINGLE_ORDER_LIMIT = 10000000

class Trade(BaseModel):
    trade_id: str
    user_id: str
    stock_symbol: str
    price: float = Field(..., gt=0, description="Price must be strictly positive")
    volume: float = Field(..., gt=0, description="Volume must be strictly positive")
    action: str        # "BUY" or "SELL"
    order_type: str    # "MARKET" or "LIMIT"

@app.post("/trades/", dependencies=[Security(get_api_key)])
def create_trade(trade: Trade, db: Session = Depends(get_db)):
    ingestion_time = time.time()
    normalized_symbol = trade.stock_symbol.upper()

    total_value = trade.price * trade.volume
    if total_value > SINGLE_ORDER_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"HIGH RISK ALERT: Trade value {total_value} exceeds limit!"
        )

    if normalized_symbol in RESTRICTED_STOCKS:
        raise HTTPException(
            status_code=403,
            detail=f"COMPLIANCE ALERT: Trading {normalized_symbol} is RESTRICTED."
        )

    new_trade = models.TradeSQL(
        trade_id=trade.trade_id,
        stock_symbol=normalized_symbol,
        price=trade.price,
        volume=trade.volume,
        user_id=trade.user_id,
        action=trade.action,
        order_type=trade.order_type
    )

    db.add(new_trade)
    db.commit()
    db.refresh(new_trade)

    try:
        trade_event = {
            "trade_id": trade.trade_id,
            "user_id": trade.user_id,
            "stock_symbol": normalized_symbol,
            "price": trade.price,
            "volume": trade.volume,
            "action": trade.action,
            "order_type": trade.order_type,
            "ingestion_timestamp": ingestion_time
        }
        kafka_producer.produce(
            topic='live_trades',
            key=trade.trade_id.encode('utf-8'),
            value=json.dumps(trade_event).encode('utf-8')
        )
        kafka_producer.poll(0)
    except Exception as e:
        print(f"KAFKA ERROR: {e}")

    return {"status": "success", "trade_id": trade.trade_id}