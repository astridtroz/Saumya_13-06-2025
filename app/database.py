from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from dotenv import load_dotenv
import os

load_dotenv()

DATABASE_URL=os.getenv("DATABASE_URL")

print(f"DEBUG: DATABASE_URL loaded: {DATABASE_URL}")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable not set. Please create a .env file.")

engine=create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal=sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base= declarative_base()

def get_db():
    db=SessionLocal()
    try:
        yield db
    finally:
        db.close()

