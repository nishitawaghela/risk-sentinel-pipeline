from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from database import Base

class TradeSQL(Base):
    __tablename__ = "trades"

    #columns
    id = Column(Integer, primary_key=True, index=True) #auto-incrementing ID (1, 2, 3...)
    trade_id = Column(String, unique=True, index=True) #the UUID from the trader
    stock_ticker = Column(String, index=True)
    price = Column(Float)
    quantity = Column(Integer)
    trader_id = Column(String)
    
    #audit trail:-exactly when did our system accept it?
    created_at = Column(DateTime, default=datetime.utcnow)


'''here, we tell the database what the "Trade" table looks like. It is almost identical to our Pydantic model, but for storage, not validation.'''