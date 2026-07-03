from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base

from dotenv import load_dotenv
import os

load_dotenv()
SQLALCHEMY_DATABASE_URL = os.getenv("SQLALCHEMY_DATABASE_URL")

#the engine (connection canager)
#check_same_thread=False is needed only for SQLite
engine = create_engine(
    SQLALCHEMY_DATABASE_URL,
    pool_size=50,        # 50 per worker = 200 total
    max_overflow=50,     # Max spike to 400 total (Safely under the 1000 limit)
    pool_timeout=30      # Queue gracefully instead of crashing
)

#he session-every time a request comes in, we open a session. when done, we close it
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#the Base (template)-all our database tables will inherit from this class
Base = declarative_base()