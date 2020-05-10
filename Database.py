import sqlite3
import datetime
from typing import List
from dateutil.relativedelta import *

MIN_RECORDS = 500

class Database:

    def __init__(self):
        self.conn = sqlite3.connect("stonks.db")
        self.create_updated_table()
        self.create_stock_table()

    def create_updated_table(self):
        """
            This table stores the last updated time of a stock.
        """
        
        CMD = """
            CREATE TABLE IF NOT EXISTS updated_on(
                symbol VARCHAR NOT NULL,
                timestamp DATE NOT NULL,
                CONSTRAINT time_unique UNIQUE (symbol, timestamp)
            );
        """
        self.execute(CMD)
    
    def create_stock_table(self):
        """
            As of now, everything goes in one mega-table.
        """

        CMD = """
            CREATE TABLE IF NOT EXISTS stocks(
                symbol VARCHAR NOT NULL,
                name VARCHAR NOT NULL,
                timestamp DATE NOT NULL,
                open DECIMAL(10,5),
                close DECIMAL(10,5),
                high DECIMAL(10,5),
                low DECIMAL(10,5),
                volume DECIMAL(10,5),
                CONSTRAINT time_unique UNIQUE (symbol, timestamp)
            );
        """
        self.execute(CMD)

    def execute(self,cmd:str):
        """ Executes without looking at a return """
        try:
            curs = self.conn.cursor()
            curs.execute(cmd)
        except Exception as e:
            print("=========================================")
            print("ERROR:")
            print(e)
            print("-----------------------------------------")
            print("CMD: {}".format(cmd))
            print("=========================================")

    def query(self, cmd:str):
        """ Executes without looking at a return """
        res = None
        try:
            curs = self.conn.cursor()
            curs.execute(cmd)
            res = curs.fetchall()
        except Exception as e:
            print("=========================================")
            print("ERROR:")
            print(e)
            print("-----------------------------------------")
            print("CMD: {}".format(cmd))
            print("=========================================")
        return res
        
    def get_symbols(self)->List[str]:
        """ Returns the list of known trading symbols """
        cmd = "SELECT DISTINCT symbol from stocks;"
        return self.query(cmd)

    def get_symbol_updated_time(self, symbol:str)->str:
        """Gets the most recent date a symbol's stock was updated"""
        cmd = "SELECT timestamp FROM stocks WHERE symbol='{}' ORDER BY timestamp DESC LIMIT 1;".format(symbol)
        return self.query(cmd)[0][0]
        
    def get_symbol_size(self,symbol:str) -> int:
        """ Returns the number of rows assocated with a stock symbol """
        cmd = "SELECT COUNT(*) FROM stocks WHERE symbol='{}';".format(symbol)
        return self.query(cmd)[0][0]

    def insert_stocks(self, row:dict):
        """
            Inserts a stock row into the database
        """

        cmd1 = """
            INSERT INTO STOCKS(symbol,name,timestamp,open,high,low,close,volume) 
            VALUES('{}','{}','{}','{}','{}','{}','{}','{}');
        """.format(row['symbol'],row['name'],row['timestamp'],row['open'],
        row['high'],row['low'],row['close'],row['volume'])

        self.execute(cmd1)

    def update(self, symbol:str):
        """Marks a symbol as having been updated today"""
        cmd = """
            INSERT INTO updated_on (symbol,timestamp) VALUES ('{}','{}');
        """.format(symbol,self.get_today())
        self.execute(cmd)

    def updated_today(self, symbol:str):
        """Returns whether or not a symbol has been updated today"""

        cmd = "select * from updated_on where symbol='{}' and timestamp='{}';".format(symbol,self.get_today())
        res = self.query(cmd)

        return len(res) != 0


    def get_today(self):
        """Returns today formatted as a YYYY-MM-DD string """
        now = datetime.datetime.now()
        year = now.year
        month = now.month
        day = now.day
        return "{}-{}-{}".format(year,month,day)
        
    def get_month_ago(self):
        """Returns one month ago today formatted as a YYYY-MM-DD string"""
        now = datetime.datetime.now()
        monthago = now - relativedelta(months=+1)
        year = monthago.year
        month = monthago.month
        if month < 10:
            month ="0{}".format(month)
        day = monthago.day
        if day < 10:
            day ="0{}".format(day)
        return "{}-{}-{}".format(year,month,day)
        
    def get_symbol_data(self, symbol:str) -> List[tuple]:
        cmd = "SELECT * FROM stocks WHERE symbol='{}';".format(symbol)
        return self.query(cmd)
        
        
    def get_relevent_symbols(self) -> List[str]:
        """ Gets a list of symbols currently being updated, and with 'enough' records """
    
        symbols = self.get_symbols()
        current_symbols = []
    
        today = self.get_today()
        month_ago = self.get_month_ago()
    
        for symbol in symbols:
            symbol = symbol[0]
            updated_time = self.get_symbol_updated_time(symbol)
        
            if month_ago <= updated_time:
            
                if self.get_symbol_size(symbol) > MIN_RECORDS:
                    current_symbols.append(symbol)
        return current_symbols
        

    def commit(self):
        self.conn.commit()


    def close(self):
        self.conn.close()

    