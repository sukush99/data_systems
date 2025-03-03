import pyodbc
import struct
from azure.identity import DefaultAzureCredential
from azure_comp.azure_config import config
from loguru import logger
import math
from tqdm import tqdm as p_bar
import decimal
# Configure logging (if needed)
logger.add("logfile.log", level="INFO")

class MainModel:
    def __init__(self): #connection string is now a parameter
        self.connection_string = (
        f"Driver={{ODBC Driver 18 for SQL Server}};"
        f"Server={config.server_name}.database.windows.net,1433;"
        f"Database={config.database_name};"
        f"UID={config.username_azure};"
        f"PWD={{{config.password_azure}}};"  # Enclose password in extra curly braces
        f"Encrypt=yes;"
        f"TrustServerCertificate=no"
    )

    def get_conn(self):
        conn = pyodbc.connect(self.connection_string)
        return conn

    def convert_decimals(self, obj):
        if isinstance(obj, dict):
            return {k: self.convert_decimals(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_decimals(item) for item in obj]
        elif isinstance(obj, decimal.Decimal):
            return float(obj)  # Convert Decimal to float
        else:
            return obj

    def insert_fact_ohlc(self, data: list):
        COLUMN_NAMES = (
            "symbol_id", "exchange_id", "timestamp_ms", "_open", "_high", "_low", "_close", "volume",
            "adj_high", "adj_low", "adj_close", "adj_open", "adj_volume", "split_factor", "dividend",
            "ADJ_SMA_10", "ADJ_SMA_20", "ADJ_SMA_50", "ADJ_SMA_100", "ADJ_SMA_200", "ADJ_EMA_12",
            "ADJ_EMA_26", "ADJ_EMA_50", "ADJ_EMA_100", "ADJ_WMA_30", "ADJ_HMA_30", "ADJ_DEMA_30",
            "ADJ_TEMA_30", "ADJ_RSI_14", "ADJ_RSI_7", "ADJ_MACD", "ADJ_MACD_signal", "ADJ_MACD_hist",
            "ADJ_Stoch_K", "ADJ_Stoch_D", "ADJ_ATR_14", "ADJ_ADX_14", "ADJ_CCI_14", "ADJ_WILLR_14",
            "ADJ_AD", "ADJ_OBV", "ADJ_SAR", "ADJ_MOM_10", "ADJ_ROC_10", "ADJ_MFI_14", "ADJ_ULTOSC",
            "ADJ_TRIX", "ADJ_KC_upper", "ADJ_KC_middle", "ADJ_KC_adj_lower", "ADJ_Aroon_Up",
            "ADJ_Aroon_Down", "ADJ_EFI_13", "ADJ_VWAP", "ADJ_Ichimoku_Tenkan_Sen", "ADJ_Ichimoku_Kijun_Sen", "ADJ_Ichimoku_Senkou_Span_A",
            "ADJ_Ichimoku_Senkou_Span_B", "ADJ_Ichimoku_Chikou_Span", "ADJ_Pivot_Point", "ADJ_R1",
            "ADJ_S1", "ADJ_R2", "ADJ_S2", "ADJ_Vortex_Plus", "ADJ_Vortex_Minus", "SMA_10", "SMA_20",
            "SMA_50", "SMA_100", "SMA_200", "EMA_12", "EMA_26", "EMA_50", "EMA_100", "WMA_30", "HMA_30",
            "DEMA_30", "TEMA_30", "RSI_14", "RSI_7", "MACD", "MACD_signal", "MACD_hist", "Stoch_K",
            "Stoch_D", "ATR_14", "ADX_14", "CCI_14", "WILLR_14", "AD", "OBV", "SAR", "MOM_10", "ROC_10",
            "MFI_14", "ULTOSC", "TRIX", "KC_upper", "KC_middle", "KC_lower", "Aroon_Up", "Aroon_Down",
            "Donchian_Upper", "Donchian_Middle", "Donchian_Lower", "CMO_14", "Stoch_RSI_K",
            "Stoch_RSI_D", "EFI_13", "VWAP", "Ichimoku_Tenkan_Sen", "Ichimoku_Kijun_Sen",
            "Ichimoku_Senkou_Span_A", "Ichimoku_Senkou_Span_B", "Ichimoku_Chikou_Span", "Pivot_Point",
            "R1", "S1", "R2", "S2", "Vortex_Plus", "Vortex_Minus"
        )

        try:
            with self.get_conn() as conn:
                crsr = conn.cursor()
                placeholders = ", ".join(["?"] * len(COLUMN_NAMES))
                insert_query = f"INSERT INTO fact_ohlc_daily ({', '.join(COLUMN_NAMES)}) VALUES ({placeholders})"
                
                with p_bar(total=len(data), desc="Inserting OHLC Data", unit="rows", bar_format="{l_bar}{bar:50}{r_bar}{bar:-50b}") as pbar:
                    for item in data:
                        values = [item[col] for col in COLUMN_NAMES]
                        # Handle NaN values
                        for i in range(len(values)):
                            if isinstance(values[i], float) and math.isnan(values[i]):
                                values[i] = None  # Replace NaN with None (NULL)
                        crsr.execute(insert_query, values)
                        pbar.update(1)
                conn.commit()
                logger.info('Data inserted successfully')
        except Exception as e:
            logger.error('Error inserting data')
            logger.error(e)

    def does_symbol_exists(self, symbol_id: str) -> bool:
        try:
            with self.get_conn() as conn:
                crsr = conn.cursor()
                crsr.execute("SELECT distinct(symbol_id) FROM fact_ohlc_daily WHERE symbol_id = ?", symbol_id)
                if crsr.fetchone():
                    return True
                else:
                    return False
        except Exception as e:
            logger.error(f"Error checking if symbol exists: {e}")
            return False

    def insert_dim_symbol(self, data: list):
        COLUMN_NAMES = ("symbol_id", "symbol_name", "cik", "isin", "employer_id", "series_id", "item_type", "sector", "industry", "sic_code", "sic_name"
        )
        try:
            with self.get_conn() as conn:
                crsr = conn.cursor()
                placeholders = ", ".join(["?"] * len(COLUMN_NAMES))
                insert_query = f"INSERT INTO dim_symbol ({', '.join(COLUMN_NAMES)}) VALUES ({placeholders})"
                
                with p_bar(total=len(data), desc="Inserting Symbol Data", unit="rows", bar_format="{l_bar}{bar:50}{r_bar}{bar:-50b}") as pbar:
                    for item in data:
                        values = [item[col] for col in COLUMN_NAMES]
                        crsr.execute(insert_query, values)
                        pbar.update(1)
                conn.commit()
                logger.info('Data inserted successfully')
        except Exception as e:
            logger.error('Error inserting data')
            logger.error(e) 

    def check_if_exixts_today(self, symbol_id: str, today_ms : int) -> bool:
        try:
            with self.get_conn() as conn:
                crsr = conn.cursor()
                crsr.execute(f"SELECT top(1) (timestamp_ms) FROM fact_ohlc_daily WHERE  symbol_id = '{symbol_id}' order by timestamp_ms desc")
                db_date = crsr.fetchone()
                if db_date == today_ms:
                    return True
                else:
                    return False
        except Exception as e:
            logger.error(f"Error checking if symbol exists: {e}")
            return False
    
    def get_200_for_live(self, symbol_id: str) -> list:
        try:
            with self.get_conn() as conn:
                crsr = conn.cursor()
                crsr.execute(f"SELECT top(200) * FROM fact_ohlc_daily WHERE symbol_id = '{symbol_id}' order by timestamp_ms desc")
                rows = crsr.fetchall()
                columns = [column[0] for column in crsr.description]
                result = [dict(zip(columns, row)) for row in rows]
                result_converted = self.convert_decimals(result)
                return result_converted
        except Exception as e:
            logger.error(f"Error getting data: {e}")
            return []

