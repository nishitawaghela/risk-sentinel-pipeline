from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker,declarative_base

#database url- we use SQLite for dev. it creates a file named trades.db
SQLALCHEMY_DATABASE_URL= "sqlite:///./trades.db"

#the engine (connection canager)
#check_same_thread=False is needed only for SQLite
engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)

#he session-every time a request comes in, we open a session. when done, we close it
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

#the Base (template)-all our database tables will inherit from this class
Base = declarative_base()