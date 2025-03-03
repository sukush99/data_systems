import os
import pyodbc, struct
from fastapi import FastAPI
from azure import identity
from pydantic import BaseModel
from loguru import logger
from azure_comp.azure_config import config


class fact_ohlc_daily(BaseModel):
    symbol_id : str
    timestamp_ms : int
    exchange_id : str
    _open : float
    _high : float
    _low : float
    _close : float
    volume : int
    adj_high : float
    adj_close : float
    adj_low : float
    adj_open : float
    adj_volume : int
    split_factor : float
    dividend : float
    ADJ_SMA_10 : float
    ADJ_SMA_20 : float
    ADJ_SMA_50 : float
    ADJ_SMA_100 : float
    ADJ_SMA_200 : float
    ADJ_EMA_12 : float
    ADJ_EMA_26 : float
    ADJ_EMA_50 : float
    ADJ_EMA_100 : float
    ADJ_WMA_30 : float
    ADJ_HMA_30 : float
    ADJ_DEMA_30 : float
    ADJ_TEMA_30 : float
    ADJ_RSI_14 : float
    ADJ_RSI_7 : float
    ADJ_MACD : float
    ADJ_MACD_signal : float
    ADJ_MACD_hist : float
    ADJ_Stoch_K : float
    ADJ_Stoch_D : float
    ADJ_ATR_14 : float
    ADJ_ADX_14 : float
    ADJ_CCI_14 : float
    ADJ_WILLR_14 : float
    ADJ_AD : float
    ADJ_OBV : float
    ADJ_SAR : float
    ADJ_MOM_10 : float
    ADJ_ROC_10 : float
    ADJ_MFI_14 : float
    ADJ_ULTOSC : float
    ADJ_TRIX : float
    ADJ_KC_upper : float
    ADJ_KC_middle : float
    ADJ_KC_adj_lower : float
    ADJ_Aroon_Up : float
    ADJ_Aroon_Down : float
    ADJ_Donchian_Upper : float
    ADJ_Donchian_Middle : float
    ADJ_Donchian_adj_lower : float
    ADJ_CMO_14 : float
    ADJ_Stoch_RSI_K : float
    ADJ_Stoch_RSI_D : float
    ADJ_EFI_13 : float
    ADJ_VWAP : float
    ADJ_Ichimoku_Tenkan_Sen : float
    ADJ_Ichimoku_Kijun_Sen : float
    ADJ_Ichimoku_Senkou_Span_A : float
    ADJ_Ichimoku_Senkou_Span_B : float
    ADJ_Ichimoku_Chikou_Span : float
    ADJ_Pivot_Point : float
    ADJ_R1 : float
    ADJ_S1 : float
    ADJ_R2 : float
    ADJ_S2 : float
    ADJ_Vortex_Plus : float
    ADJ_Vortex_Minus : float
    SMA_10 : float
    SMA_20 : float
    SMA_50 : float
    SMA_100 : float
    SMA_200 : float
    EMA_12 : float
    EMA_26 : float
    EMA_50 : float
    EMA_100 : float
    WMA_30 : float
    HMA_30 : float
    DEMA_30 : float
    TEMA_30 : float
    RSI_14 : float
    RSI_7 : float
    MACD : float
    MACD_signal : float
    MACD_hist : float
    Stoch_K : float
    Stoch_D : float
    ATR_14 : float
    ADX_14 : float
    CCI_14 : float
    WILLR_14 : float
    AD : float
    OBV : float
    SAR : float
    MOM_10 : float
    ROC_10 : float
    MFI_14 : float
    ULTOSC : float
    TRIX : float
    KC_upper : float
    KC_middle : float
    KC_lower : float
    Aroon_Up : float
    Aroon_Down : float
    Donchian_Upper : float
    Donchian_Middle : float
    Donchian_Lower : float
    CMO_14 : float
    Stoch_RSI_K : float
    Stoch_RSI_D : float
    EFI_13 : float
    VWAP : float
    Ichimoku_Tenkan_Sen : float
    Ichimoku_Kijun_Sen : float
    Ichimoku_Senkou_Span_A : float
    Ichimoku_Senkou_Span_B : float
    Ichimoku_Chikou_Span : float
    Pivot_Point : float
    R1 : float
    S1 : float
    R2 : float
    S2 : float
    Vortex_Plus : float
    Vortex_Minus : float

#creating an app
app = FastAPI()

class MainModel:
    def __init__(self):
        self.connection_string = config.azure_sql_connection_string

    def get_conn(self):
        credential = identity.DefaultAzureCredential(exclude_interactive_browser_credential=False)
        token_bytes = credential.get_token("https://database.windows.net/.default").token.encode("UTF-16-LE")
        token_struct = struct.pack(f'<I{len(token_bytes)}s', len(token_bytes), token_bytes)
        SQL_COPT_SS_ACCESS_TOKEN = 1256  # This connection option is defined by microsoft in msodbcsql.h
        conn = pyodbc.connect(self.connection_string, attrs_before={SQL_COPT_SS_ACCESS_TOKEN: token_struct})
        return conn


def get_db():
    db = MainModel()
    try: 
        yield db
    finally:
        pass


@app.post("/insert_fact_ohlc")
def insert_fact_ohlc(data: list[fact_ohlc_daily], db: MainModel = Depends(get_db)):
    try:
        with get_conn() as conn:
            crsr = conn.cursor()
            for item in data:
                logger.info(f'Inserting data for {item.symbol_id}')
                crsr.execute(
                    "INSERT INTO fact_ohlc_daily (symbol_id,timestamp_ms,exchange_id,address_id,open,high,low,close,volume,adj_high,adj_close,adj_low,adj_open,adj_volume,split_factor,dividend,ADJ_SMA_10,ADJ_SMA_20,ADJ_SMA_50,ADJ_SMA_100,ADJ_SMA_200,ADJ_EMA_12,ADJ_EMA_26,ADJ_EMA_50,ADJ_EMA_100,ADJ_WMA_30,ADJ_HMA_30,ADJ_DEMA_30,ADJ_TEMA_30,ADJ_RSI_14,ADJ_RSI_7,ADJ_MACD,ADJ_MACD_signal,ADJ_MACD_hist,ADJ_Stoch_K,ADJ_Stoch_D,ADJ_ATR_14,ADJ_ADX_14,ADJ_CCI_14,ADJ_WILLR_14,ADJ_AD,ADJ_OBV,ADJ_SAR,ADJ_MOM_10,ADJ_ROC_10,ADJ_MFI_14,ADJ_ULTOSC,ADJ_TRIX,ADJ_KC_upper,ADJ_KC_middle,ADJ_KC_adj_lower,ADJ_Aroon_Up,ADJ_Aroon_Down,ADJ_Donchian_Upper,ADJ_Donchian_Middle,ADJ_Donchian_adj_lower,ADJ_CMO_14,ADJ_Stoch_RSI_K,ADJ_Stoch_RSI_D,ADJ_EFI_13,ADJ_VWAP,ADJ_Ichimoku_Tenkan_Sen,ADJ_Ichimoku_Kijun_Sen,ADJ_Ichimoku_Senkou_Span_A,ADJ_Ichimoku_Senkou_Span_B,ADJ_Ichimoku_Chikou_Span,ADJ_Pivot_Point,ADJ_R1,ADJ_S1,ADJ_R2,ADJ_S2,ADJ_Vortex_Plus,ADJ_Vortex_Minus,SMA_10,SMA_20,SMA_50,SMA_100,SMA_200,EMA_12,EMA_26,EMA_50,EMA_100,WMA_30,HMA_30,DEMA_30,TEMA_30,RSI_14,RSI_7,MACD,MACD_signal,MACD_hist,Stoch_K,Stoch_D,ATR_14,ADX_14,CCI_14,WILLR_14,AD,OBV,SAR,MOM_10,ROC_10,MFI_14,ULTOSC,TRIX,KC_upper,KC_middle,KC_lower,Aroon_Up,Aroon_Down,Donchian_Upper,Donchian_Middle,Donchian_Lower,CMO_14,Stoch_RSI_K,Stoch_RSI_D,EFI_13,VWAP,Ichimoku_Tenkan_Sen,Ichimoku_Kijun_Sen,Ichimoku_Senkou_Span_A,Ichimoku_Senkou_Span_B,Ichimoku_Chikou_Span,Pivot_Point,R1,S1,R2,S2,Vortex_Plus,Vortex_Minus) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    item[0],item[1],item[2],item[3],item[4],item[5],item[6],item[7],item[8],item[9],item[10],item[11],item[12],item[13],item[14],item[15],item[16],item[17],item[18],item[19],item[20],item[21],item[22],item[23],item[24],item[25],item[26],item[27],item[28],item[29],item[30],item[31],item[32],item[33],item[34],item[35],item[36],item[37],item[38],item[39],item[40],item[41],item[42],item[43],item[44],item[45],item[46],item[47],item[48],item[49],item[50],item[51],item[52],item[53],item[54],item[55],item[56],item[57],item[58],item[59],item[60],item[61],item[62],item[63],item[64],item[65],item[66],item[67],item[68],item[69],item[70],item[71],item[72],item[73],item[74],item[75],item[76],item[77],item[78],item[79],item[80],item[81],item[82],item[83],item[84],item[85],item[86],item[87],item[88],item[89],item[90],item[91],item[92],item[93],item[94],item[95],item[96],item[97],item[98],item[99],item[100],item[101],item[102],item[103],item[104],item[105],item[106],item[107],item[108],item[109],item[110],item[111],item[112],item[113],item[114],item[115],item[116],item[117],item[118],item[119],item[120],item[121],item[122],item[123],item[124],item[125],item[126],item[127],item[128],item[129]
                                )
            conn.commit()
            logger.info(f'Data inserted for {data.symbol_id}')
    except Exception as e:
        logger.error(f'Error inserting data for {data.symbol_id}')
        logger.error(e)
        return {"status": "error"}
    return {"status": "success"}
                



