"""
General application settings.

This file defines default values that can be overridden by environment variables.
"""
import os
import os.path

# Determine project root - using os.path instead of Path for simplicity
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Environment
ENVIRONMENT = os.getenv("ENVIRONMENT", "development")
DEBUG = os.getenv("DEBUG", "False").lower() in ("true", "yes", "1", "y")

# Logging
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
LOG_FILE = os.getenv("LOG_FILE", os.path.join(PROJECT_ROOT, "logs", "crypto_arbitrage.log"))
LOG_FORMAT = os.getenv("LOG_FORMAT", "%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s")

# Database
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cryptouser:cryptouser123@localhost:5432/crypto_arbitrage")
DB_POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "10"))
DB_MAX_OVERFLOW = int(os.getenv("DB_MAX_OVERFLOW", "20"))
DB_POOL_TIMEOUT = int(os.getenv("DB_POOL_TIMEOUT", "30"))
DB_POOL_RECYCLE = int(os.getenv("DB_POOL_RECYCLE", "1800"))
DB_CONNECT_TIMEOUT = int(os.getenv("DB_CONNECT_TIMEOUT", "10"))
DB_ISOLATION_LEVEL = os.getenv("DB_ISOLATION_LEVEL", "READ COMMITTED")

# API
API_HOST = os.getenv("API_HOST", "0.0.0.0")
API_PORT = int(os.getenv("API_PORT", "5000"))
API_PREFIX = os.getenv("API_PREFIX", "/api")
API_CORS_ORIGINS = os.getenv("API_CORS_ORIGINS", "*").split(",")

# Data Collection
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
USER_AGENT = os.getenv("USER_AGENT", "Crypto-Arbitrage-Tracker/1.0")

# Snapshot Schedule
SNAPSHOT_INTERVAL_MINUTES = int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "5"))
CONCURRENT_COLLECTORS = int(os.getenv("CONCURRENT_COLLECTORS", "5"))

# Assets to track
ASSETS = os.getenv("ASSETS", "USDT,BTC,ETH,TON").split(",")
QUOTE_ASSETS = os.getenv("QUOTE_ASSETS", "USDT,USD").split(",")

# Cache settings
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", "3600"))  # 1 hour
CACHE_MAX_SIZE = int(os.getenv("CACHE_MAX_SIZE", "1000"))