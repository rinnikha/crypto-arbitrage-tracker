# config/settings.py
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# General settings
DEBUG = os.getenv("DEBUG", "False").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Database settings
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///crypto_arbitrage.db")

# Scheduler settings
SNAPSHOT_INTERVAL_MINUTES = int(os.getenv("SNAPSHOT_INTERVAL_MINUTES", "5"))

# UI settings
UI_PORT = int(os.getenv("UI_PORT", "8050"))
UI_HOST = os.getenv("UI_HOST", "0.0.0.0")

# API request settings
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
USER_AGENT = "Crypto-Arbitrage-Tracker/1.0"