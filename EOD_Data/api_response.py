import requests
from datetime import datetime
from loguru import logger
from config import config
from TA_addon import calculate_technical_indicators
import pandas as pd
from tqdm import tqdm as p_bar

def save_data_to_csv(processed_data, filename="processed_data.csv"):
    try:
        # Convert processed_data (list of dictionaries) to a DataFrame
        df = pd.DataFrame(processed_data)
        
        # Save the DataFrame to a CSV file
        df.to_csv(filename, index=False)
        logger.info(f"Data successfully saved to {filename}")
    except Exception as e:
        logger.error(f"Error saving data to CSV: {e}")


def process_data(data: dict):
    grouped_data = []
    logger.info("Processing stock data into structured format.")
    
    for stock_data in p_bar(data.get("data", []), desc="Processing stocks", ncols=100):
        try:
            date_obj = datetime.strptime(stock_data["date"], "%Y-%m-%dT%H:%M:%S%z")
        except Exception as e:
            logger.error(f"Date parsing error: {e} for stock: {stock_data.get('symbol')}")
            continue
        
        timestamp_ms = int(date_obj.timestamp() * 1000)
        grouped_data.append({
            "open": stock_data["open"],
            "high": stock_data["high"],
            "low": stock_data["low"],
            "close": stock_data["close"],
            "volume": stock_data["volume"],
            "adj_high": stock_data["adj_high"],
            "adj_low": stock_data["adj_low"],
            "adj_close": stock_data["adj_close"],
            "adj_open": stock_data["adj_open"],
            "adj_volume": stock_data["adj_volume"],
            "split_factor": stock_data["split_factor"],
            "dividend": stock_data["dividend"],
            "symbol": stock_data["symbol"],
            "exchange": stock_data["exchange"],
            "timestamp_ms": timestamp_ms,
            "date": stock_data["date"],
        })
    
    if not grouped_data:
        logger.warning("No grouped data available after processing.")
        return None
    
    return grouped_data if len(grouped_data) > 1 else grouped_data[0]


def fetch_stock_data(access_key: str, symbol: str):
    """
    Fetches stock data from marketstack API and processes it.
    """
    all_stock_data = []
    
    logger.info(f"Fetching stock data for symbol: {symbol} from source: {config.data_source}")
    
    if config.data_source == "live":
        url = "http://api.marketstack.com/v1/eod/latest"
        params = {"access_key": access_key, "symbols": symbol}
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            if response.status_code == 200:
                data_live = response.json()
                logger.debug(f"Live data fetched for {symbol}: {data_live}")
                return process_data(data_live)
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching live data for {symbol}: {e}")
            return None
    
    elif config.data_source == "backfill":
        OFFSETS = config.offsets if hasattr(config, "offsets") else [1, 92, 183, 274]
        CACHE_KEY_BASE = f"stock_data_{symbol}"
        combined_data = []

        for offset in p_bar(OFFSETS, desc="Fetching backfill data", ncols=100):
            logger.info(f"Fetching backfill data for offset {offset} for symbol: {symbol}")
            url = "http://api.marketstack.com/v1/eod"
            params = {
                "access_key": access_key,
                "symbols": symbol,
                "limit": config.limit,
                "offset": offset,
            }
            try:
                response = requests.get(url, params=params)
                response.raise_for_status()
                
                if response.status_code == 200:
                    data_backfill = response.json()
                    #logger.debug(f"Data for offset {offset}: {data_backfill}")
                    ready_data = process_data(data_backfill)
                    
                    if ready_data is None:
                        logger.warning(f"No data returned for offset {offset} for symbol: {symbol}")
                        continue
                    else:
                        combined_data.extend([ready_data] if isinstance(ready_data, dict) else ready_data)
                else:
                    logger.error(f"Error fetching data for offset {offset}: {response.text}")
                    return None
            except requests.exceptions.RequestException as e:
                logger.error(f"Error fetching data for offset {offset} for symbol: {symbol}: {e}")
                continue
        
        # Debug combined data before calculating technical indicators
        #logger.debug(f"Combined data before processing technical indicators: {combined_data}")
        processed_data = calculate_technical_indicators(combined_data)
        logger.info(f"Processed technical indicators for {symbol}")
        return processed_data if len(processed_data) > 1 else processed_data[0]
    
    else:
        logger.error(f"Invalid data source: {config.data_source}")
        return None
