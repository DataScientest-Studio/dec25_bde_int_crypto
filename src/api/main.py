"""
FastAPI server for Crypto Data API.

This API provides:
- Grafana-compatible endpoints for time-series visualization
- ML prediction endpoints for price forecasting
"""

import logging

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from src.api.routers import grafana, prediction
from src.config.mongo_settings import get_settings

# Get configuration from mongo settings
mongo_settings = get_settings()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(
    title="Crypto Data API",
    version="1.0.0",
    description="API for cryptocurrency data visualization and price prediction"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(grafana.router)
app.include_router(prediction.router)


@app.get("/")
def root():
    """Health check endpoint."""
    return {
        "status": "ok",
        "message": "Crypto Data API",
        "version": "1.0.0",
        "endpoints": {
            "grafana": "/grafana/*",
            "prediction": "/predict/*",
            "docs": "/docs"
        }
    }


def main():
    """Run the API server."""
    logger.info(f"Starting Crypto Data API server on http://0.0.0.0:8000")
    logger.info(f"MongoDB URI: {mongo_settings.mongodb_uri}")
    logger.info(f"Interactive docs available at: http://0.0.0.0:8000/docs")
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
