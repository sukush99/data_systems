# from sqlalchemy import create_engine, Column, String, Integer, Float, BigInteger, ForeignKey
# from sqlalchemy.orm import declarative_base, relationship, sessionmaker
# from azure_comp.azure_config import config
# from azure.identity import DefaultAzureCredential
# from loguru import logger
# import struct



# credential = DefaultAzureCredential()
# Base = declarative_base()

# @event.listens_for(engine, "do_connect")
# def provide_token(dialect, conn_rec, cargs, cparams):
#     token = credential.get_token("https://database.windows.net/.default").token.encode("utf-16-le")
#     token_struct = struct.pack(f"<I{len(token)}s", len(token), token)
#     SQL_COPT_SS_ACCESS_TOKEN = 1256  # Connection option defined in msodbcsql.h
#     cparams["attrs_before"] = {SQL_COPT_SS_ACCESS_TOKEN: token_struct}
    
# #Define the Symbol Dimension Table
# class dim_symbol(Base):
#     __tablename__ = 'dim_symbol'
#     symbol_id = Column(String(255), primary_key=True)
#     symbol_name = Column(String(255))
#     cik = Column(String(255))
#     isin = Column(String(255))
#     cusip = Column(String(255))
#     ein_employer_id = Column(String(255))
#     lei = Column(String(255))
#     series_id = Column(String(255))
#     item_type = Column(String(255))
#     sector = Column(String(255))
#     industry = Column(String(255))
#     sic_code = Column(String(255))
#     sic_name = Column(String(255))

# #Define the Timestamp Dimension Table
# # Define the Timestamp Dimension Table
# class dim_timestamp(Base):
#     __tablename__ = "dim_timestamp"
    
#     timestamp_ms = Column(BigInteger, primary_key=True)
#     date = Column(String(50))
#     day_of_the_week = Column(String(50))
#     month = Column(Integer)
#     year = Column(Integer)
#     quarter = Column(Integer)
#     fiscal_year = Column(Integer)
#     is_weekend = Column(Integer)
#     is_public_holiday = Column(Integer)

# # Define the Exchange Dimension Table
# class dim_exchange(Base):
#     __tablename__ = "dim_exchange"
    
#     exchange_id = Column(String(255), primary_key=True)
#     exchange_name = Column(String(255))
#     acronym = Column(String(50))
#     country_code = Column(String(10))
#     city = Column(String(255))
#     market_category_code = Column(String(50))
#     exchange_status = Column(String(50))

# # Define the Fact Table
# class fact_ohlc_daily(Base):
#         __tablename__ = "fact_ohlc_daily"
        
#         symbol_id = Column(String(255), ForeignKey("dim_symbol.symbol_id"), primary_key=True)
#         timestamp_ms = Column(BigInteger, ForeignKey("dim_timestamp.timestamp_ms"), primary_key=True)
#         exchange_id = Column(String(255), ForeignKey("dim_exchange.exchange_id"), primary_key=True)
        
#         _open = Column(Float)
#         _high = Column(Float)
#         _low = Column(Float)
#         _close = Column(Float)
#         volume = Column(BigInteger)
#         adj_high = Column(Float)
#         adj_close = Column(Float)
#         adj_low = Column(Float)
#         adj_open = Column(Float)
#         adj_volume = Column(BigInteger)
#         split_factor = Column(Float)
#         dividend = Column(Float)
#         ADJ_SMA_10 = Column(Float)
#         ADJ_SMA_20 = Column(Float)
#         ADJ_SMA_50 = Column(Float)
#         ADJ_SMA_100 = Column(Float)
#         ADJ_SMA_200 = Column(Float)
#         ADJ_EMA_12 = Column(Float)
#         ADJ_EMA_26 = Column(Float)
#         ADJ_EMA_50 = Column(Float)
#         ADJ_EMA_100 = Column(Float)
#         ADJ_WMA_30 = Column(Float)
#         ADJ_HMA_30 = Column(Float)
#         ADJ_DEMA_30 = Column(Float)
#         ADJ_TEMA_30 = Column(Float)
#         ADJ_RSI_14 = Column(Float)
#         ADJ_RSI_7 = Column(Float)
#         ADJ_MACD = Column(Float)
#         ADJ_MACD_signal = Column(Float)
#         ADJ_MACD_hist = Column(Float)
#         ADJ_Stoch_K = Column(Float)
#         ADJ_Stoch_D = Column(Float)
#         ADJ_ATR_14 = Column(Float)
#         ADJ_ADX_14 = Column(Float)
#         ADJ_CCI_14 = Column(Float)
#         ADJ_WILLR_14 = Column(Float)
#         ADJ_AD = Column(Float)
#         ADJ_OBV = Column(Float)
#         ADJ_SAR = Column(Float)
#         ADJ_MOM_10 = Column(Float)
#         ADJ_ROC_10 = Column(Float)
#         ADJ_MFI_14 = Column(Float)
#         ADJ_ULTOSC = Column(Float)
#         ADJ_TRIX = Column(Float)
#         ADJ_KC_upper = Column(Float)
#         ADJ_KC_middle = Column(Float)
#         ADJ_KC_adj_lower = Column(Float)
#         ADJ_Aroon_Up = Column(Float)
#         ADJ_Aroon_Down = Column(Float)
#         ADJ_Donchian_Upper = Column(Float)
#         ADJ_Donchian_Middle = Column(Float)
#         ADJ_Donchian_adj_lower = Column(Float)
#         ADJ_CMO_14 = Column(Float)
#         ADJ_Stoch_RSI_K = Column(Float)
#         ADJ_Stoch_RSI_D = Column(Float)
#         ADJ_EFI_13 = Column(Float)
#         ADJ_VWAP = Column(Float)
#         ADJ_Ichimoku_Tenkan_Sen = Column(Float)
#         ADJ_Ichimoku_Kijun_Sen = Column(Float)
#         ADJ_Ichimoku_Senkou_Span_A = Column(Float)
#         ADJ_Ichimoku_Senkou_Span_B = Column(Float)
#         ADJ_Ichimoku_Chikou_Span = Column(Float)
#         ADJ_Pivot_Point = Column(Float)
#         ADJ_R1 = Column(Float)
#         ADJ_S1 = Column(Float)
#         ADJ_R2 = Column(Float)
#         ADJ_S2 = Column(Float)
#         ADJ_Vortex_Plus = Column(Float)
#         ADJ_Vortex_Minus = Column(Float)
#         SMA_10 = Column(Float)
#         SMA_20 = Column(Float)
#         SMA_50 = Column(Float)
#         SMA_100 = Column(Float)
#         SMA_200 = Column(Float)
#         EMA_12 = Column(Float)
#         EMA_26 = Column(Float)
#         EMA_50 = Column(Float)
#         EMA_100 = Column(Float)
#         WMA_30 = Column(Float)
#         HMA_30 = Column(Float)
#         DEMA_30 = Column(Float)
#         TEMA_30 = Column(Float)
#         RSI_14 = Column(Float)
#         RSI_7 = Column(Float)
#         MACD = Column(Float)
#         MACD_signal = Column(Float)
#         MACD_hist = Column(Float)
#         Stoch_K = Column(Float)
#         Stoch_D = Column(Float)
#         ATR_14 = Column(Float)
#         ADX_14 = Column(Float)
#         CCI_14 = Column(Float)
#         WILLR_14 = Column(Float)
#         AD = Column(Float)
#         OBV = Column(Float)
#         SAR = Column(Float)
#         MOM_10 = Column(Float)
#         ROC_10 = Column(Float)
#         MFI_14 = Column(Float)
#         ULTOSC = Column(Float)
#         TRIX = Column(Float)
#         KC_upper = Column(Float)
#         KC_middle = Column(Float)
#         KC_lower = Column(Float)
#         Aroon_Up = Column(Float)
#         Aroon_Down = Column(Float)
#         Donchian_Upper = Column(Float)
#         Donchian_Middle = Column(Float)
#         Donchian_Lower = Column(Float)
#         CMO_14 = Column(Float)
#         Stoch_RSI_K = Column(Float)
#         Stoch_RSI_D = Column(Float)
#         EFI_13 = Column(Float)
#         VWAP = Column(Float)
#         Ichimoku_Tenkan_Sen = Column(Float)
#         Ichimoku_Kijun_Sen = Column(Float)
#         Ichimoku_Senkou_Span_A = Column(Float)
#         Ichimoku_Senkou_Span_B = Column(Float)
#         Ichimoku_Chikou_Span = Column(Float)
#         Pivot_Point = Column(Float)
#         R1 = Column(Float)
#         S1 = Column(Float)
#         R2 = Column(Float)
#         S2 = Column(Float)
#         Vortex_Plus = Column(Float)
#         Vortex_Minus = Column(Float)


#         # Define Relationships (Optional)
#         symbol = relationship("dim_symbol")
#         timestamp = relationship("dim_timestamp")
#         exchange = relationship("dim_exchange")

#     #create the engine for Azure SQL

# class MainModel:
#     def __init__(self):
#         self.DATABASE_URL = config.azure_sql_connection_string
#         self.engine = create_engine(self.DATABASE_URL, echo=True)
#         self.Session = sessionmaker(bind=self.engine)
#         self.session = self.Session()

#     def APIToTablefact_ohlc_daily(self, data: list) -> bool:
#         if isinstance(data,list):
#             sucess = True
#             for record in data:
#                 try:
#                     new_record = fact_ohlc_daily(**record)
#                     #logger.info(f"Data to be added to fact_ohlc_daily table: {record}")
#                     self.session.add(new_record)
#                     #logger.info(f"Data added to fact_ohlc_daily table: {record}")
#                     self.session.commit()
#                     logger.info("Data committed to fact_ohlc_daily table.")
#                 except Exception as e:
#                     logger.error(f"Error adding data to fact_ohlc_daily table: {e}")
#                     self.session.rollback()
#                     success = False
#             return success
#         else:
#             try:
#                 new_record = fact_ohlc_daily(**data)
#                 logger.info(f"Data to be added to fact_ohlc_daily table: {data}")
#                 self.session.add(new_record)
#                 logger.info(f"Data added to fact_ohlc_daily table: {data}")
#                 self.session.commit()
#                 logger.info("Data committed to fact_ohlc_daily table.")
#                 return True
#             except Exception as e:
#                 logger.error(f"Error adding data to fact_ohlc_daily table: {e}")
#                 self.session.rollback()
#                 return False

#     # def APIToTabledim_symbol(self, data) -> bool:
#     #     try:
#     #         #TODO: convert the dict to a class object
#     #         session.add(data)
#     #         logger.info(f"Data added to dim_symbol table: {data}")
#     #         session.commit()
#     #         logger.info("Data committed to dim_symbol table.")
#     #         return True
#     #     except Exception as e:
#     #         logger.error(f"Error adding data to dim_symbol table: {e}")
#     #         return False

#     # def APIToTabledim_timestamp(self, data) -> bool:
#     #     try:
#     #         #TODO: convert the dict to a class object
#     #         session.add(data)
#     #         logger.info(f"Data added to dim_timestamp table: {data}")
#     #         session.commit()
#     #         logger.info("Data committed to dim_timestamp table.")
#     #         return True
#     #     except Exception as e:
#     #         logger.error(f"Error adding data to dim_timestamp table: {e}")
#     #         return False

#     # def APIToTabledim_exchange(self, data) -> bool:
#     #     try:
#     #         #TODO: convert the dict to a class object
#     #         session.add(data)
#     #         logger.info(f"Data added to dim_exchange table: {data}")
#     #         session.commit()
#     #         logger.info("Data committed to dim_exchange table.")
#     #         return True
#     #     except Exception as e:
#     #         logger.error(f"Error adding data to dim_exchange table: {e}")
#     #         return False

#     # def FetchDataFromfact_ohlc_daily(self, no_of_rows: int, symbol_id: str):
#     #     try:
#     #         data = session.query(fact_ohlc_daily).limit(no_of_rows).filter(symbol_id = symbol_id).all()
#     #         return data
#     #     except Exception as e:
#     #         logger.error(f"Error fetching data from fact_ohlc_daily table: {e}")
#     #         return None
    
#     # def FetchDataFromdim_symbols(self, search_symbol: str):
#     #     try:   
#     #         data = session.query(dim_symbol).filter(dim_symbol.symbol_id == search_symbol).all()
#     #         return data
#     #     except Exception as e:
#     #         logger.error(f"Error fetching data from dim_symbol table: {e}")
#     #         return None

