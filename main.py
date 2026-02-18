from fastapi import FastAPI , HTTPException , Security , status
from fastapi.security import APIKeyHeader
import os
import secrets
from dotenv import load_dotenv
from pydantic import BaseModel, Field 

#load secrets from .env 
API_KEY_SECRET=os.getenv("API_KEY")
API_KEY_NAME = "X-UBS-Client-Key"

#if key is missing, crash immediately 
if not API_KEY_SECRET:
    raise RuntimeError("CRITICAL ERROR: UBS_API_KEY not found in environment!")

app = FastAPI(title="UBS Sentinel: Trade Ingestion API")

api_key_header= APIKeyHeader(name=API_KEY_NAME , auto_error=False)

async def get_api_key(api_key_header: str = Security(api_key_header)):
    #constant time comparison prevents Timing Attacks
    if api_key_header and secrets.compare_digest(api_key_header, API_KEY_SECRET):
        return api_key_header
    else:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Could not validate credentials (Access Denied)"
        )
    
#logic rules
RESTRICTED_STOCKS={"UBS" , "UBSG" , "SCAM_CO" , "BLOCKED_LTD"}
SINGLE_ORDER_LIMIT= 10000000

class Trade(BaseModel):
    trade_id : str
    stock_ticker: str
    price : float = Field(...,gt=0 , description="Price must be strictly positive")
    quantity : float = Field(...,gt=0 , description="Quantity must be strictly positive")
    trader_id : str

#security gate

@app.post("/trades/" , dependencies =[Security(get_api_key)])
def create_trade(trade : Trade):
    #rule 1 - fat finger check
    total_value=trade.price * trade.quantity
    if total_value > SINGLE_ORDER_LIMIT:
        raise HTTPException(
            status_code=403,
            detail=f"COMPLIANCE ALERT: Trading {trade.stock_ticker} is RESTRICTED."
        )
    #success-trade passes
    return {
        "status": "Trade Accepted",
        "trade_id": trade.trade_id,
        "message": "Logged to Silver Layer"
    }
