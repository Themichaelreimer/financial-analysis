from typing import List
from Database import *
from statsmodels.tsa.arima_model import ARIMA
import datetime



def parse_date(date:str) -> datetime.datetime:
    return datetime.datetime.strptime(date,"%Y-%m-%d")

def first_order_difference(data:list) -> list:
    """ Performs first order differencing on a list of data."""
    res = []
    for i in range(len(data)-1):
        res.append((data[i+1][0],data[i+1][1] - data[i][1]))
    return res
    
def proc_data(data:List[dict]) -> List[tuple]:
    """ Formats data for use in analysis algorithms """
    result = []
    for item in data:
        result.append( (parse_date(item[2]),item[6]) )
    return first_order_difference(result)
    
def count_times_positive(data:list) -> int:
    times = 0
    for x in data:
        if x>0:
            times += 1
        elif x<0:
            times -= 1
    return times

def analysis_1():
    db = Database()
    symbols = db.get_relevent_symbols()
    summaries = []
    for symbol in symbols:
        data = db.get_symbol_data(symbol)
        data = proc_data(data)
        summaries.append((symbol,count_times_positive([x[1] for x in data])))
        
    summaries.sort(key=lambda x:x[1], reverse=True)
    
    print("5 Best:")
    print(summaries[0:5])
    print("5 Worst:")
    print(summaries[-5:])
        #model = ARIMA(data, order=(5,1,0))
        #model_fit = model.fit(disp=0)
        #print(model_fit.summary())
        
        
analysis_1()