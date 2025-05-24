cat README.md
# Data Systems Project

## Overview
A comprehensive data pipeline system designed to collect, transform, and analyze financial data from multiple sources. The project consists of three main components that work together to provide a complete financial data processing solution.

## Components

### 1. EOD Data Component
- Fetches end-of-day stock market data using an API
- Processes and stores data in Azure SQL Database
- Features both live and backfill data collection modes
- Includes technical analysis add-ons
- Handles dimension tables for symbols and timestamps

### 2. Edgar Component
- Retrieves SEC EDGAR filings (10-K and 10-Q forms)
- Processes XBRL data from financial statements
- Converts filings into structured CSV format
- Stores processed data in Azure Blob Storage
- Maintains filing indices for easy access

### 3. Data Transform Component
- Transforms financial data stored in Azure Blob Storage
- Processes company-specific files
- Handles multiple file types and formats
- Implements error handling and logging
- Supports batch processing for multiple companies

## Project Structure
```
data_systems/
├── data_transform/
│   ├── config.env
│   ├── config.py
│   ├── Makefile
│   ├── pyproject.toml
│   ├── README.md
│   ├── run.py
│   ├── settings.env
│   ├── transform.py
│   └── utils.py
├── docker-compose/
│   ├── docker-backfill.yml
│   ├── docker-daily.yml
│   └── makefile
├── edgar/
│   ├── auth_key.env
│   ├── backfill.env
│   ├── config.py
│   ├── Makefile
│   ├── pyproject.toml
│   ├── README.md
│   ├── run.py
│   └── settings.env
└── EOD_Data/
    ├── api_key.env
    ├── api_response.py
    ├── azure_comp/
    │   ├── azure_config.py
    │   ├── azure_sql.py
    │   ├── connection.py
    │   └── conn.env
    ├── config.py
    ├── dim_symbol_api.py
    ├── dim_timestamp.py
    ├── dockerfile
    ├── local_backfill.env
    ├── local_daily.env
    ├── Makefile
    ├── pyproject.toml
    ├── README.md
    ├── run.py
    ├── settings.env
    ├── TA_addon.py
    └── today_date.py
```

## Setup and Installation

### Prerequisites
- Python 3.12+
- Azure Account with:
  - Azure Blob Storage
  - Azure SQL Database
- API keys for:
  - EOD Data API
  - SEC EDGAR access

### Installation
1. Clone the repository
2. Set up virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```
3. Install dependencies for each component:
```bash
cd data_transform
pip install -e .
cd ../edgar
pip install -e .
cd ../EOD_Data
pip install -e .
```

## Configuration

Each component requires specific environment files:

### EOD Data
- `api_key.env`: API access credentials
- `settings.env`: General configuration including:
  - company_list
  - data_source (live/backfill)
  - api_access_key
  - limit and offsets (optional)
- `local_daily.env`: Daily run settings
- `local_backfill.env`: Backfill settings

### Edgar
- `auth_key.env`: SEC EDGAR authentication
- `settings.env`: Configuration including:
  - Azure storage credentials
  - SQL connection string
  - Container names
  - Company list
  - Form types (10-K, 10-Q)
- `backfill.env`: Backfill settings

### Data Transform
- `config.env`: General configuration
- `settings.env`: Configuration including:
  - Container name
  - File list
  - Company list
  - Connection strings

## Usage

### Using Docker Compose
```bash
cd docker-compose
# For daily runs
make daily
# For backfill operations
make backfill
```

### Manual Execution
Each component can be run independently:

```bash
# EOD Data
cd EOD_Data
python run.py

# Edgar
cd edgar
python run.py

# Data Transform
cd data_transform
python run.py
```

## Docker Integration
The project includes Docker support for both daily and backfill operations:
- `docker-daily.yml`: Configuration for daily data collection
- `docker-backfill.yml`: Configuration for historical data backfilling
- Uses Makefile for simplified orchestration

## Dependencies
Core dependencies include:
- pydantic-settings: Environment configuration management
- azure-storage-blob: Azure Blob Storage operations
- azure-sql: Database operations
- pandas: Data manipulation and analysis
- loguru: Logging framework
- tqdm: Progress bar visualization

## Error Handling
The project implements comprehensive error handling:
- Detailed logging using loguru
- Exception handling for:
  - API calls and responses
  - File operations
  - Database operations
  - Data transformation processes
- Progress tracking with tqdm
- Robust error reporting

## Contributing
1. Fork the repository
2. Create a feature branch
3. Commit changes
4. Push to the branch
5. Create a Pull Request

## License
[MIT License]
