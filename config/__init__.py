# config/__init__.py
"""
Configuration package for Crypto Arbitrage Tracker.
"""
from config.manager import get_config, ConfigManager

__all__ = ['get_config', 'ConfigManager']