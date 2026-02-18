from fastapi import FastAPI, HTTPException, Security, status, Depends
from fastapi.security import APIKeyHeader
from sqlalchemy.orm import Session
import os
import secrets
from dotenv import load_dotenv
from pydantic import BaseModel, Field 
import models
from database import SessionLocal, engine

#create the database tables 
models.Base.metadata.create_all(bind=engine)

#load secrets from .env 
load_dotenv()  # <--- Added this to actually load the file
API_KEY_SECRET=os.getenv("UBS_API_KEY") # Ensure .env has UBS_API_KEY
API_KEY_NAME = "X-UBS-Client-Key"

#if key is missing, crash immediately 
if not API_KEY_SECRET:
    raise RuntimeError("CRITICAL ERROR: UBS_API_KEY not found in environment!")

app = FastAPI(title="UBS Sentinel: Trade Ingestion API")

#database dependency
#this opens a connection for the request, and closes it when done
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

#security
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
def create_trade(trade : Trade, db: Session = Depends(get_db)): # <--- Fixed: Added db injection
    #convert input to uppercase - ubs or UBS would mean the same
    normalized_ticker = trade.stock_ticker.upper()
    
    #rule 1 - fat finger check
    total_value=trade.price * trade.quantity
    if total_value > SINGLE_ORDER_LIMIT:
        raise HTTPException(
            status_code=400, # Fixed: 400 for bad requests (limits)
            detail=f"HIGH RISK ALERT: Trade value {total_value} exceeds limit!" # Fixed: Correct message
        )
    
    #rule 2 - case insensitive 
    if normalized_ticker in RESTRICTED_STOCKS:
        raise HTTPException(
            status_code=403, 
            detail=f"COMPLIANCE ALERT: Trading {normalized_ticker} is RESTRICTED."
        )
    
    #persistence
    #convert Pydantic Trade -> SQL Trade
    new_trade = models.TradeSQL(
        trade_id=trade.trade_id,
        stock_ticker=normalized_ticker, # Fixed: used normalized_ticker (ticker was undefined)
        price=trade.price,
        quantity=trade.quantity,
        trader_id=trade.trader_id
    )
    
    db.add(new_trade)  #add to "Staging"
    db.commit()        #save permanently (The "Enter" key)
    db.refresh(new_trade) #get the generated ID

    #success-trade passes
    return {
        "status": "Trade Accepted",
        "trade_id": trade.trade_id,
        "database_id": new_trade.id,
        "message": "Logged to Silver Layer"
    }