crypto-arbitrage-tracker/
├── .env                      # Environment variables (API keys)
├── requirements.txt          # Python dependencies
├── main.py                   # Entry point for running the application
├── config/                   # Configuration files
│   ├── __init__.py
│   ├── settings.py           # General settings
│   └── exchanges.py          # Exchange-specific settings
├── core/                     # Core functionality
│   ├── __init__.py
│   ├── models.py             # Data models (using ORM)
│   └── utils.py              # Utility functions
├── data_collection/          # Data collection modules
│   ├── __init__.py
│   ├── base.py               # Base class for data collectors
│   ├── api_clients/          # API-based collectors
│   │   ├── __init__.py
│   │   ├── binance.py
│   │   ├── bitget.py
│   │   ├── bybit.py
│   │   ├── mexc.py
│   │   └── ton_wallet.py
│   └── scrapers/             # Web scrapers (when APIs unavailable)
│       ├── __init__.py
│       └── generic_scraper.py
├── data_storage/             # Data storage modules
│   ├── __init__.py
│   ├── database.py           # Database connection
│   ├── repositories.py       # Data access layer
│   └── snapshots.py          # Snapshot management
├── analysis/                 # Data analysis modules
│   ├── __init__.py
│   ├── liquidity.py          # Liquidity analysis
│   └── opportunities.py      # Opportunity identification
├── ui/                       # User interface
│   ├── __init__.py
│   ├── app.py                # Main UI application
│   ├── dashboard.py          # Dashboard components
│   ├── templates/            # HTML templates
│   └── static/               # Static assets
└── scheduler/                # Scheduling for periodic tasks
    ├── __init__.py
    └── jobs.py               # Scheduled jobs