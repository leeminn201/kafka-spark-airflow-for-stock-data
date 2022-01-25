import uuid
from pyspark.sql.functions import pandas_udf, PandasUDFType
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp
import pandas as pd

def _rename_columns(df):
    """ Rename column to match FINTA API requirement
    """
    columns = {"Close": 'close', "High": 'high', "Low": 'low', 'Volume': 'volume', 'Open': 'open'}

    if isinstance(columns, dict):
        return df.select(*[F.col(col_name).alias(columns.get(col_name, col_name)) for col_name in df.columns])
    else:
        raise ValueError("'columns' should be a dict, like {'old_name_1':'new_name_1', 'old_name_2':'new_name_2'}")
    
    df = df.withColumn("Date",to_timestamp(df.Date))
    
    return df


def _exponential_smooth(key, df):
    """
    Function that exponentially smooths dataset so values are less 'rigid'
    
    Args:
        key: group by key, no need to put in
        df: grouped by dataframe by "symbol"
    """
    symbol = df.Symbol.unique()[0]
    #symbol= key
    df.set_index("Date",inplace=True)
    df = df.ewm(alpha=0.65).mean()
    df.reset_index(inplace=True)
    df.loc[:,'Symbol'] = symbol
    
    return pd.DataFrame(df.values)


def _get_indicator_grouped_data(key, group):
    """
    Function that change raw data to indicator
    
    Args:
        key: group by key, no need to put in
        group: grouped by dataframe by "symbol"
    """
    
    from finta import TA
    INDICATORS = ['RSI', 'STOCH','ADL', 'ATR', 'MOM', 'MFI', 'ROC', 'OBV', 'CCI', 'EMV','WILLIAMS','ADX', 'TRIX']
    
    for indicator in INDICATORS:
        ind_data = eval('TA.' + indicator + '(group)')
        if not isinstance(ind_data,pd.DataFrame):
            ind_data = ind_data.to_frame()
            group = group.merge(ind_data, left_index=True, right_index=True)

    del (group['open'])
    del (group['high'])
    del (group['low'])
    del (group['volume'])
    del (group['Adj_close'])
    
    #uncomment 2 lines below to test return column
    #print(group.columns)
    #return pd.DataFrame([key]) 
    
    return pd.DataFrame(group.values)

def _produce_prediction(key, group):
    """
    Function that produces the 'truth' values
    At a given row, it looks 'day' rows ahead to see if the price increased (up) or decreased (down).
    When the price change less than p%, it's sideways (sw).
    
    Args:
        p: % sideways
    """
    
    #day = [3,5,7,10]
    #for d in day:
    #Wrong calculation when i use for loop and i have no idea
    
    p = 0.05
    pred_3 =  1 - ( group["close"] - group.shift(-3)["close"])/group["close"] 
    pred_3 = pred_3.iloc[:-3]
    pred_3 = pd.DataFrame(["down" if x < (1-p) else ("up" if x > (1+p) else "sw") for x in pred_3])
    group.loc[:,'pred_3_5p'] = pred_3
    
    pred_5 =  1 - ( group["close"] - group.shift(-5)["close"])/group["close"] 
    pred_5 = pred_5.iloc[:-5]
    pred_5 = pd.DataFrame(["down" if x < (1-p) else ("up" if x > (1+p) else "sw") for x in pred_5])
    group.loc[:,'pred_5_5p'] = pred_5
    
    pred_7 =  1 - ( group["close"] - group.shift(-7)["close"])/group["close"] 
    pred_7 = pred_7.iloc[:-7]
    pred_7 = pd.DataFrame(["down" if x < (1-p) else ("up" if x > (1+p) else "sw") for x in pred_7]) 
    group.loc[:,'pred_7_5p'] = pred_7
    
    pred_10 =  1 - ( group["close"] - group.shift(-10)["close"])/group["close"] 
    pred_10 = pred_10.iloc[:-10]
    pred_10 = pd.DataFrame(["down" if x < (1-p) else ("up" if x > (1+p) else "sw") for x in pred_10])
    group.loc[:,'pred_10_5p'] = pred_10


    group.dropna(inplace=True)
   
    return pd.DataFrame(group.values)


def _fully_preprocess(df):
    SCHEMA_EXP= "Date string, open double, high double, low double, close double, \
        Adj_close double, volume long, Symbol string"
    df_exp = df.groupBy("Symbol").applyInPandas(_exponential_smooth,schema=SCHEMA_EXP)
    
    SCHEMA_IND = "Date string, close double, symbol string, 14_period_RSI double, \
        14_period_STOCH_K double, MFV double, 14_period_ATR double, MOM double, 14_period_MFI double, \
            ROC double, OBV double, 20_period_CCI double, 14_period_EMV double, \
                Williams double, 14_period_ADX double, 20_period_TRIX double"
    df_indi = df_exp.groupBy("Symbol").applyInPandas(_get_indicator_grouped_data,schema=SCHEMA_IND)
    
    SCHEMA_PREDICTION = "Date string, close double, symbol string, 14_period_RSI double, \
    14_period_STOCH_K double, MFV double, 14_period_ATR double, MOM double, 14_period_MFI double, \
    ROC double, OBV double, 20_period_CCI double, 14_period_EMV double, Williams double,\
    14_period_ADX double, 20_period_TRIX double, pred_3_5p string, pred_5_5p string, pred_7_5p string, pred_10_5p string"
    
    pp_df = df_indi.groupBy("Symbol").applyInPandas(_produce_prediction,schema=SCHEMA_PREDICTION)
    
    return pp_df
