# config/settings.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# General settings
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database settings
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://cryptouser:cryptouser123@localhost:5432/crypto_arbitrage")

# Scheduler settings
SNAPSHOT_INTERVAL_MINUTES = int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "5"))

# API settings (replacing UI settings)
API_PORT = int(os.getenv("API_PORT", "5000"))
API_HOST = os.getenv("API_HOST", "0.0.0.0")

UI_PORT = int(os.getenv("UI_PORT", "3000"))
UI_HOST = os.getenv("UI_HOST", "0.0.0.0")

# Frontend settings
FRONTEND_URL = os.getenv("FRONTEND_URL", "http://localhost:3000")

# API request settings
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
USER_AGENT = "Crypto-Arbitrage-Tracker/1.0"