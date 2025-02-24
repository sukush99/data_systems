import talib as ta
import pandas as pd
import numpy as np


# First create individual Series/DataFrames for each indicator
def calculate_technical_indicators(df):
    df = pd.DataFrame(df)
    df = df.iloc[::-1]

    indicators = {}

    # Moving Averages - Normal Price
    indicators["ADJ_SMA_10"] = ta.SMA(df["adj_close"], 10)
    indicators["ADJ_SMA_20"] = ta.SMA(df["adj_close"], 20)
    indicators["ADJ_SMA_50"] = ta.SMA(df["adj_close"], 50)
    indicators["ADJ_SMA_100"] = ta.SMA(df["adj_close"], 100)
    indicators["ADJ_SMA_200"] = ta.SMA(df["adj_close"], 200)
    indicators["ADJ_EMA_12"] = ta.EMA(df["adj_close"], 12)
    indicators["ADJ_EMA_26"] = ta.EMA(df["adj_close"], 26)
    indicators["ADJ_EMA_50"] = ta.EMA(df["adj_close"], 50)
    indicators["ADJ_EMA_100"] = ta.EMA(df["adj_close"], 100)
    indicators["ADJ_WMA_30"] = ta.WMA(df["adj_close"], 30)
    indicators["ADJ_HMA_30"] = ta.WMA(
        df["adj_close"], 30
    )  # Approximate Hull Moving Average
    indicators["ADJ_DEMA_30"] = ta.DEMA(df["adj_close"], 30)
    indicators["ADJ_TEMA_30"] = ta.TEMA(df["adj_close"], 30)

    # RSI (Relative Strength Index)
    indicators["ADJ_RSI_14"] = ta.RSI(df["adj_close"], timeperiod=14)
    indicators["ADJ_RSI_7"] = ta.RSI(df["adj_close"], timeperiod=7)

    # MACD (Moving Average Convergence Divergence)
    adj_macd, adj_macd_signal, adj_macd_hist = ta.MACD(df["adj_close"])
    indicators["ADJ_MACD"] = adj_macd
    indicators["ADJ_MACD_signal"] = adj_macd_signal
    indicators["ADJ_MACD_hist"] = adj_macd_hist

    # Stochastic Oscillator
    stoch_k, stoch_d = ta.STOCH(df["adj_high"], df["adj_low"], df["adj_close"])
    indicators["ADJ_Stoch_K"] = stoch_k
    indicators["ADJ_Stoch_D"] = stoch_d

    # ATR (Average True Range) - Measures volatility
    indicators["ADJ_ATR_14"] = ta.ATR(
        df["adj_high"], df["adj_low"], df["adj_close"], timeperiod=14
    )

    # ADX (Average Directional Index) - Measures trend strength
    indicators["ADJ_ADX_14"] = ta.ADX(
        df["adj_high"], df["adj_low"], df["adj_close"], timeperiod=14
    )

    # CCI (Commodity Channel Index) - Measures momentum
    indicators["ADJ_CCI_14"] = ta.CCI(
        df["adj_high"], df["adj_low"], df["adj_close"], timeperiod=14
    )

    # Williams %R
    indicators["ADJ_WILLR_14"] = ta.WILLR(
        df["adj_high"], df["adj_low"], df["adj_close"], timeperiod=14
    )

    # Chaikin A/D Line (Accumulation/Distribution)
    indicators["ADJ_AD"] = ta.AD(
        df["adj_high"], df["adj_low"], df["adj_close"], df["adj_volume"]
    )

    # On-Balance adj_volume (OBV)
    indicators["ADJ_OBV"] = ta.OBV(df["adj_close"], df["adj_volume"])

    # Parabolic SAR
    indicators["ADJ_SAR"] = ta.SAR(df["adj_high"], df["adj_low"])

    # Momentum Indicator
    indicators["ADJ_MOM_10"] = ta.MOM(df["adj_close"], timeperiod=10)

    # ROC (Rate of Change)
    indicators["ADJ_ROC_10"] = ta.ROC(df["adj_close"], timeperiod=10)

    # MFI (Money Fadj_low Index) - Measures buying and selling pressure
    indicators["ADJ_MFI_14"] = ta.MFI(
        df["adj_high"], df["adj_low"], df["adj_close"], df["adj_volume"], timeperiod=14
    )

    # Ultimate Oscillator
    indicators["ADJ_ULTOSC"] = ta.ULTOSC(df["adj_high"], df["adj_low"], df["adj_close"])

    # TRIX (Triple Exponential Average)
    indicators["ADJ_TRIX"] = ta.TRIX(df["adj_close"])

    # Keltner Channel
    kc_middle = ta.EMA(df["adj_close"], 20)
    kc_upper = kc_middle + 2 * ta.ATR(
        df["adj_high"], df["adj_low"], df["adj_close"], timeperiod=10
    )
    kc_adj_lower = kc_middle - 2 * ta.ATR(
        df["adj_high"], df["adj_low"], df["adj_close"], timeperiod=10
    )
    indicators["ADJ_KC_upper"] = kc_upper
    indicators["ADJ_KC_middle"] = kc_middle
    indicators["ADJ_KC_adj_lower"] = kc_adj_lower

    # Aroon Indicator
    aroon_up, aroon_down = ta.AROON(df["adj_high"], df["adj_low"], timeperiod=14)
    indicators["ADJ_Aroon_Up"] = aroon_up
    indicators["ADJ_Aroon_Down"] = aroon_down

    # Donchian Channel
    donchian_upper = df["adj_high"].rolling(window=20).max()
    donchian_adj_lower = df["adj_low"].rolling(window=20).min()
    donchian_middle = (donchian_upper + donchian_adj_lower) / 2
    indicators["ADJ_Donchian_Upper"] = donchian_upper
    indicators["ADJ_Donchian_Middle"] = donchian_middle
    indicators["ADJ_Donchian_adj_lower"] = donchian_adj_lower

    # Chande Momentum Oscillator (CMO)
    indicators["ADJ_CMO_14"] = ta.CMO(df["adj_close"], timeperiod=14)

    # Stochastic RSI
    stochrsi_k, stochrsi_d = ta.STOCHRSI(df["adj_close"])
    indicators["ADJ_Stoch_RSI_K"] = stochrsi_k
    indicators["ADJ_Stoch_RSI_D"] = stochrsi_d

    # Elder's Force Index
    indicators["ADJ_EFI_13"] = df["adj_close"].diff(1) * df["adj_volume"]

    # VWAP (adj_volume Weighted Average Price)
    vwap = (df["adj_close"] * df["adj_volume"]).cumsum() / df["adj_volume"].cumsum()
    indicators["ADJ_VWAP"] = vwap

    # Ichimoku Cloud
    tenkan_sen = (df["adj_high"].rolling(9).max() + df["adj_low"].rolling(9).min()) / 2
    kijun_sen = (df["adj_high"].rolling(26).max() + df["adj_low"].rolling(26).min()) / 2
    senkou_span_a = (tenkan_sen + kijun_sen) / 2
    senkou_span_b = (
        df["adj_high"].rolling(52).max() + df["adj_low"].rolling(52).min()
    ) / 2
    chikou_span = df["adj_close"].shift(-26)

    indicators["ADJ_Ichimoku_Tenkan_Sen"] = tenkan_sen
    indicators["ADJ_Ichimoku_Kijun_Sen"] = kijun_sen
    indicators["ADJ_Ichimoku_Senkou_Span_A"] = senkou_span_a
    indicators["ADJ_Ichimoku_Senkou_Span_B"] = senkou_span_b
    indicators["ADJ_Ichimoku_Chikou_Span"] = chikou_span

    # Pivot Points
    pivot = (df["adj_high"] + df["adj_low"] + df["adj_close"]) / 3
    r1 = (2 * pivot) - df["adj_low"]
    s1 = (2 * pivot) - df["adj_high"]
    r2 = pivot + (df["adj_high"] - df["adj_low"])
    s2 = pivot - (df["adj_high"] - df["adj_low"])

    indicators["ADJ_Pivot_Point"] = pivot
    indicators["ADJ_R1"] = r1
    indicators["ADJ_S1"] = s1
    indicators["ADJ_R2"] = r2
    indicators["ADJ_S2"] = s2

    # Vortex Indicator
    vi_plus = ta.PLUS_DI(df["adj_high"], df["adj_low"], df["adj_close"], timeperiod=14)
    vi_minus = ta.MINUS_DI(
        df["adj_high"], df["adj_low"], df["adj_close"], timeperiod=14
    )
    indicators["ADJ_Vortex_Plus"] = vi_plus
    indicators["ADJ_Vortex_Minus"] = vi_minus

    # Moving Averages - Normal
    indicators["SMA_10"] = ta.SMA(df["close"], 10)
    indicators["SMA_20"] = ta.SMA(df["close"], 20)
    indicators["SMA_50"] = ta.SMA(df["close"], 50)
    indicators["SMA_100"] = ta.SMA(df["close"], 100)
    indicators["SMA_200"] = ta.SMA(df["close"], 200)
    indicators["EMA_12"] = ta.EMA(df["close"], 12)
    indicators["EMA_26"] = ta.EMA(df["close"], 26)
    indicators["EMA_50"] = ta.EMA(df["close"], 50)
    indicators["EMA_100"] = ta.EMA(df["close"], 100)
    indicators["WMA_30"] = ta.WMA(df["close"], 30)
    indicators["HMA_30"] = ta.WMA(df["close"], 30)  # Approximate Hull Moving Average
    indicators["DEMA_30"] = ta.DEMA(df["close"], 30)
    indicators["TEMA_30"] = ta.TEMA(df["close"], 30)

    # RSI (Relative Strength Index)
    indicators["RSI_14"] = ta.RSI(df["close"], timeperiod=14)
    indicators["RSI_7"] = ta.RSI(df["close"], timeperiod=7)

    # MACD (Moving Average Convergence Divergence)
    macd, macd_signal, macd_hist = ta.MACD(df["close"])
    indicators["MACD"] = macd
    indicators["MACD_signal"] = macd_signal
    indicators["MACD_hist"] = macd_hist

    # Stochastic Oscillator
    stoch_k, stoch_d = ta.STOCH(df["high"], df["low"], df["close"])
    indicators["Stoch_K"] = stoch_k
    indicators["Stoch_D"] = stoch_d

    # ATR (Average True Range) - Measures volatility
    indicators["ATR_14"] = ta.ATR(df["high"], df["low"], df["close"], timeperiod=14)

    # ADX (Average Directional Index) - Measures trend strength
    indicators["ADX_14"] = ta.ADX(df["high"], df["low"], df["close"], timeperiod=14)

    # CCI (Commodity Channel Index) - Measures momentum
    indicators["CCI_14"] = ta.CCI(df["high"], df["low"], df["close"], timeperiod=14)

    # Williams %R
    indicators["WILLR_14"] = ta.WILLR(df["high"], df["low"], df["close"], timeperiod=14)

    # Chaikin A/D Line (Accumulation/Distribution)
    indicators["AD"] = ta.AD(df["high"], df["low"], df["close"], df["volume"])

    # On-Balance Volume (OBV)
    indicators["OBV"] = ta.OBV(df["close"], df["volume"])

    # Parabolic SAR
    indicators["SAR"] = ta.SAR(df["high"], df["low"])

    # Momentum Indicator
    indicators["MOM_10"] = ta.MOM(df["close"], timeperiod=10)

    # ROC (Rate of Change)
    indicators["ROC_10"] = ta.ROC(df["close"], timeperiod=10)

    # MFI (Money Flow Index) - Measures buying and selling pressure
    indicators["MFI_14"] = ta.MFI(
        df["high"], df["low"], df["close"], df["volume"], timeperiod=14
    )

    # Ultimate Oscillator
    indicators["ULTOSC"] = ta.ULTOSC(df["high"], df["low"], df["close"])

    # TRIX (Triple Exponential Average)
    indicators["TRIX"] = ta.TRIX(df["close"])

    # Keltner Channel
    kc_middle = ta.EMA(df["close"], 20)
    kc_upper = kc_middle + 2 * ta.ATR(df["high"], df["low"], df["close"], timeperiod=10)
    kc_lower = kc_middle - 2 * ta.ATR(df["high"], df["low"], df["close"], timeperiod=10)
    indicators["KC_upper"] = kc_upper
    indicators["KC_middle"] = kc_middle
    indicators["KC_lower"] = kc_lower

    # Aroon Indicator
    aroon_up, aroon_down = ta.AROON(df["high"], df["low"], timeperiod=14)
    indicators["Aroon_Up"] = aroon_up
    indicators["Aroon_Down"] = aroon_down

    # Donchian Channel
    donchian_upper = df["high"].rolling(window=20).max()
    donchian_lower = df["low"].rolling(window=20).min()
    donchian_middle = (donchian_upper + donchian_lower) / 2
    indicators["Donchian_Upper"] = donchian_upper
    indicators["Donchian_Middle"] = donchian_middle
    indicators["Donchian_Lower"] = donchian_lower

    # Chande Momentum Oscillator (CMO)
    indicators["CMO_14"] = ta.CMO(df["close"], timeperiod=14)

    # Stochastic RSI
    stochrsi_k, stochrsi_d = ta.STOCHRSI(df["close"])
    indicators["Stoch_RSI_K"] = stochrsi_k
    indicators["Stoch_RSI_D"] = stochrsi_d

    # Elder's Force Index
    indicators["EFI_13"] = df["close"].diff(1) * df["volume"]

    # VWAP (Volume Weighted Average Price)
    vwap = (df["close"] * df["volume"]).cumsum() / df["volume"].cumsum()
    indicators["VWAP"] = vwap

    # Ichimoku Cloud
    tenkan_sen = (df["high"].rolling(9).max() + df["low"].rolling(9).min()) / 2
    kijun_sen = (df["high"].rolling(26).max() + df["low"].rolling(26).min()) / 2
    senkou_span_a = (tenkan_sen + kijun_sen) / 2
    senkou_span_b = (df["high"].rolling(52).max() + df["low"].rolling(52).min()) / 2
    chikou_span = df["close"].shift(-26)

    indicators["Ichimoku_Tenkan_Sen"] = tenkan_sen
    indicators["Ichimoku_Kijun_Sen"] = kijun_sen
    indicators["Ichimoku_Senkou_Span_A"] = senkou_span_a
    indicators["Ichimoku_Senkou_Span_B"] = senkou_span_b
    indicators["Ichimoku_Chikou_Span"] = chikou_span

    # Pivot Points
    pivot = (df["high"] + df["low"] + df["close"]) / 3
    r1 = (2 * pivot) - df["low"]
    s1 = (2 * pivot) - df["high"]
    r2 = pivot + (df["high"] - df["low"])
    s2 = pivot - (df["high"] - df["low"])

    indicators["Pivot_Point"] = pivot
    indicators["R1"] = r1
    indicators["S1"] = s1
    indicators["R2"] = r2
    indicators["S2"] = s2

    # Vortex Indicator
    vi_plus = ta.PLUS_DI(df["high"], df["low"], df["close"], timeperiod=14)
    vi_minus = ta.MINUS_DI(df["high"], df["low"], df["close"], timeperiod=14)
    indicators["Vortex_Plus"] = vi_plus
    indicators["Vortex_Minus"] = vi_minus

    result = pd.concat(indicators, axis=1)

    # Replace None values with NaN
    result = result.replace([None], np.nan)

    # Join with original dataframe
    return pd.concat([df, result], axis=1).to_dict(orient="records")
