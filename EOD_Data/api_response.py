import requests
from datetime import datetime
from loguru import logger
from config import config
from TA_addon import calculate_technical_indicators
import pandas as pd
from tqdm import tqdm as p_bar
from azure_comp.connection import ConnectToAzure
from azure_comp.azure_sql import MainModel

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
            "symbol_id": stock_data["symbol"],
            "timestamp_ms": timestamp_ms,
            "exchange_id": stock_data["exchange"],
            "_open": stock_data["open"],
            "_high": stock_data["high"],
            "_low": stock_data["low"],
            "_close": stock_data["close"],
            "volume": stock_data["volume"],
            "adj_high": stock_data["adj_high"],
            "adj_low": stock_data["adj_low"],
            "adj_close": stock_data["adj_close"],
            "adj_open": stock_data["adj_open"],
            "adj_volume": stock_data["adj_volume"],
            "split_factor": stock_data["split_factor"],
            "dividend": stock_data["dividend"],
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
        #inintialize the model
        db = MainModel()
        debug = True
        #TODO: first check if the archivce folder has the historical data fill

        if debug == False:
            None
        else:
            OFFSETS = config.offsets if hasattr(config, "offsets") else [1, 92, 183, 274]
            CACHE_KEY_BASE = f"{symbol}_historical_data"
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
                retries = 4
                for attempt in range(1, retries + 1):
                    try:
                        response = requests.get(url, params=params)
                        response.raise_for_status()
                        
                        if response.status_code == 200:
                            data_backfill = response.json()
                            ready_data = process_data(data_backfill)
                            
                            if ready_data is None:
                                logger.warning(f"No data returned for offset {offset} for symbol: {symbol}")
                            else:
                                logger.info(f"Received data from process_data as expected")
                                combined_data.extend([ready_data] if isinstance(ready_data, dict) else ready_data)
                                
                                Add_to_sql = True
                            break  # Successful response, break out of retry loop
                        else:
                            logger.error(f"Error fetching data for offset {offset}: {response.text}")
                    except requests.exceptions.RequestException as e:
                        logger.error(f"Attempt {attempt} failed: Error fetching data for offset {offset} for symbol: {symbol}: {e}")
                        if attempt == retries:
                            logger.error(f"Failed to fetch data for offset {offset} after {retries} attempts.")

        while Add_to_sql:
            logger.info(f"Data fetched for {symbol}. Processing technical indicators.")
            processed_data = calculate_technical_indicators(combined_data)
            logger.info(f"Processed technical indicators for {symbol}")
            # try:
            #     api_url = "http://127.0.0.1:8000/insert_fact_ohlc"  #api URL

            #     #send the data to the API
            #     headers = {"Content-Type": "application/json"}
            #     response = requests.post(api_url, json=processed_data, headers=headers)
            #     response.raise_for_status()

            #     if response.json().get("status") == "success":
            #         logger.info(f"Data added to fact_ohlc_daily table for {symbol}")
            #         success = True
            #     else:
            #         logger.error(f"Error adding data to fact_ohlc_daily table for {symbol}")
            #         success = False
            # except requests.exceptions.RequestException as e:
            #     logger.error(f"Error adding data to fact_ohlc_daily table for {symbol}: {e}")
            #     success = False
            db.insert_fact_ohlc(processed_data)
            break
    else:
        logger.error(f"Invalid data source: {config.data_source}")
        return None
