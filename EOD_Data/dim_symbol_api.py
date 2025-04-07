import requests
from loguru import logger
from azure_comp.azure_sql import MainModel

class APIModel:
    '''
    Class to fetch symbol information from the API
    Args:
        symbol: symbol to fetch information for
        api_key: API key to use for fetching data
    '''
    def __init__(self):
        pass

    def get_symbol_exchange_info(self, symbol: str, api_key: str) -> dict:
        '''
        Fetches symbol information from the API
        Args:
            symbol: symbol to fetch information for
            api_key: API key to use for fetching data
        Returns:
            dict: symbol information
        '''
        try:
            url = f"http://api.marketstack.com/v2/tickers/{symbol}?access_key={api_key}"
            logger.info(f"API URL: {url}")
            response = requests.get(url)
            if response.status_code == 200:
                returned_data = response.json()
                #for exchange
                data_xch_format = []
                data_xch = returned_data.get("stock_exchange", [])
                logger.debug(f"Data fetched for symbol: {symbol}: {returned_data}")
                data_xch_format.append({
                    "exchange_id": data_xch["mic"],
                    "exchange_name": data_xch["name"],
                    "acronym": data_xch["acronym"],
                    "country_code": data_xch["country_code"],
                    "city": data_xch["city"],
                    "market_category_code": data_xch["market_category_code"],
                    "exchange_status": data_xch["exchange_status"]
                })
                logger.debug(f"Exchange info for {symbol}: {data_xch_format}")
                logger.debug(f"Data fetched for symbol: {symbol}: {returned_data}")
                symbol_val = []
                table_val = []
                for key in returned_data.keys():
                    if key != "stock_exchange":
                        symbol_info = returned_data.get(key, {})
                        symbol_val.append(symbol_info)
                        logger.debug(f"Symbol info for {symbol}: {symbol_info}")
                    else:
                        logger.info(f"simply skipping stock_exchange data")
                        pass
                table_val.append({
                    "symbol_id": symbol_val[1],
                    "symbol_name": symbol_val[0],
                    "cik": symbol_val[2],
                    "isin": symbol_val[3],
                    "employer_id": symbol_val[5],
                    "series_id": symbol_val[7],
                    "item_type": symbol_val[8],
                    "sector": symbol_val[9],
                    "industry": symbol_val[10],
                    "sic_code": symbol_val[11],
                    "sic_name": symbol_val[12]
                })
                #call insert_symbol_exchange
                
            else:
                logger.error(f"inside Error fetching data for symbol: {symbol}. Status code: {response.status_code}")
                return {}
        except Exception as e:
            logger.error(f"main Error fetching data for symbol: {symbol}. Error: {str(e)}")
            return {}
        
        db = MainModel()
        logger.info(f"Inserting data for symbol: {symbol}")
        db.insert_symbol_exchange(symbol_data=table_val, exchange_data=data_xch_format)


    