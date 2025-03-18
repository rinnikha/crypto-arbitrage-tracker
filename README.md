# Crypto Arbitrage Tracker

A sophisticated application for monitoring cryptocurrency prices across exchanges and identifying arbitrage opportunities.

## Project Structure

```
crypto-arbitrage-tracker/
├── .env                      # Environment variables (API keys)
├── requirements.txt          # Python dependencies
├── main.py                   # Entry point
├── app.py                    # Main application class
├── config/                   # Configuration
│   ├── __init__.py
│   ├── manager.py            # Configuration manager
│   ├── settings.py           # General settings
│   └── exchanges.py          # Exchange-specific settings
├── core/                     # Core functionality
│   ├── __init__.py
│   ├── models.py             # Database models
│   ├── dto.py                # Data Transfer Objects
│   ├── http.py               # HTTP client utilities
│   ├── cache.py              # Caching utilities
│   ├── concurrency.py        # Concurrency utilities
│   ├── errors.py             # Error handling and logging
│   └── utils.py              # Utility functions
├── data_collection/          # Data collection
│   ├── __init__.py
│   ├── base_collector.py     # Base collector classes
│   ├── api_clients/          # API-based collectors
│   │   ├── __init__.py
│   │   ├── binance_collector.py
│   │   ├── bitget.py
│   │   ├── bybit.py
│   │   ├── mexc.py
│   │   └── ton_wallet.py
│   └── scrapers/             # Web scrapers
│       ├── __init__.py
│       └── generic_scraper.py
├── data_storage/             # Data storage
│   ├── __init__.py
│   ├── database.py           # Database connection
│   ├── base_repository.py    # Base repository
│   ├── p2p_repository.py     # P2P repository
│   ├── spot_repository.py    # Spot repository
│   └── snapshot_managers.py  # Snapshot management
├── analysis/                 # Data analysis
│   ├── __init__.py
│   ├── liquidity.py          # Liquidity analysis
│   └── opportunities.py      # Opportunity identification
└── api/                      # API
    ├── __init__.py
    └── app.py                # API application
```

## Key Components

### Core Infrastructure

- **Configuration**: Centralized configuration management with environment fallbacks
- **Error Handling**: Consistent error handling with custom exceptions
- **Logging**: Enhanced logging with useful context
- **HTTP Client**: Thread-safe HTTP client with proper retry logic
- **Caching**: Thread-safe caching for performance optimization
- **Concurrency**: Utilities for safe concurrent operations

### Data Layer

- **Data Transfer Objects (DTOs)**: Clean data objects for transferring between layers
- **Repositories**: Data access layer with optimized batch operations
- **Database Models**: SQLAlchemy ORM models

### Collection Logic

- **Base Collectors**: Base classes for different collection strategies
- **API Clients**: Exchange-specific API clients
- **Web Scrapers**: For exchanges without proper APIs

### Application Logic

- **Snapshot Managers**: Thread-safe snapshot creation
- **Analysis Tools**: Tools for identifying opportunities and analyzing liquidity
- **API**: RESTful API for accessing data and analysis

## Improvements Made

- **Cleaner Class Hierarchy**: Better inheritance for collectors and repositories
- **Thread Safety**: Improved thread safety throughout
- **Error Handling**: Consistent error handling with proper context
- **Performance**: Optimized batch operations for database
- **Caching**: Added caching to reduce database load
- **Logging**: Enhanced logging with proper context
- **Configuration**: Centralized configuration management

## Requirements

- Python 3.8+
- PostgreSQL 13+
- Dependencies listed in requirements.txt

## Setup

1. Clone the repository
2. Install dependencies: `pip install -r requirements.txt`
3. Create a `.env` file with your configuration (see `.env.example`)
4. Run database migrations: `alembic upgrade head`
5. Start the application: `python main.py`

## Development

### Adding a New Exchange

1. Create a new collector in `data_collection/api_clients/` by extending the appropriate base class
2. Add exchange settings to `config/exchanges.py`
3. Register the collector in `app.py`

### Database Migrations

Run migrations with Alembic:

```bash
# Create a new migration
alembic revision --autogenerate -m "Description"

# Apply migrations
alembic upgrade head
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.