from contextlib import asynccontextmanager
from fastapi import FastAPI
from app.api.report import router as report_router
from app.database import Base, engine 

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("Creating database tables...")
    Base.metadata.create_all(bind=engine)
    print("Database tables created (or already exist).")

    yield

    print("Shutting down database connection...")

    engine.dispose()
    print("Database connection closed.")


app = FastAPI(
    title="ShopWatch Monitoring API",
    description="Backend API for monitoring store online/offline status and generating reports.",
    version="0.1.0",
    lifespan=lifespan, 
)

app.include_router(report_router)

@app.get("/")
async def read_root():
    return {"message": "Welcome to ShopWatch API!"}