from sqlalchemy import Column, Integer, String, Float, DateTime
from datetime import datetime
from database import Base

class TradeSQL(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, index=True)
    trade_id = Column(String, unique=True, index=True)
    user_id = Column(String)
    stock_symbol = Column(String, index=True)
    price = Column(Float)
    volume = Column(Float)
    action = Column(String)
    order_type = Column(String)
    created_at = Column(DateTime, default=datetime.utcnow)