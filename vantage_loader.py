import requests
from Database import *
import time
import sys

API_KEY = '' # Get a free or paid API key at https://www.alphavantage.co
URL = 'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={}&apikey={}&outputsize={}&datatype=csv'

# 500 requests per day, 5 per minute

def history():
    return "--history" in sys.argv

def get_url(symbol:str) -> str:
    if history():
        return URL.format(symbol,API_KEY,"full")
    else:
        return URL.format(symbol,API_KEY,"compact")

def get(symbol:str):
    url = get_url(symbol)
    res = requests.get(url)

    if res.status_code == 200:
        rows = res.text.split("\r\n")
        assert rows[0] == 'timestamp,open,high,low,close,volume', rows[0]
        return rows[1:]
    else:
        print("=====================================")
        print("Request code: {}".format(res.status_code))
        print("Response: "+res.text)
        print("=====================================")

def load_symbol(db, symbol:str, name:str):
    rows = get(symbol)

    for row in rows:
        if row=="":
            continue

        rowd = dict()
        tokens = row.split(",")

        assert len(tokens) == 6, row

        rowd['timestamp'] = tokens[0]
        rowd['open'] = tokens[1]
        rowd['high'] = tokens[2]
        rowd['low'] = tokens[3]
        rowd['close'] = tokens[4]
        rowd['volume'] = tokens[5]
        rowd['symbol'] = symbol
        rowd['name'] = name

        db.insert_stocks(rowd)
    
    db.update(symbol)

def run_from_symbols_file(file_name):
    db = Database()

    f = open(file_name,'r')
    lines = f.readlines()

    for line in lines:
        tokens = line.split('\t')
        symbol = tokens[0]
        name = tokens[1]
        if "'" in name:
            name = name.replace("'","")
        

        if not db.updated_today(symbol):
            try:
                print("Loading {} - {}".format(name,symbol))
                load_symbol(db,symbol,name)
                db.commit()
            except Exception as e:
                errfile = open("err.txt", "a")
                errfile.write("{},{}: {}".format(name,symbol,e.__str__()))
        else:
            print("Skipping {}: Already seen today".format(symbol))
        

        print("Completed. Waiting 2 seconds")
        if not history():
            time.sleep(2)
    db.close()


run_from_symbols_file("NASDAQ.txt")
