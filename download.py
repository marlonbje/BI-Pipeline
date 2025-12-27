from storing import Storing
import yfinance as yf
from fredapi import Fred
import pandas as pd
import logging
import json

class Download:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def _store(self,name,db,df):
        Storing(name,db).store(df)
        
    def get_ohlcv(self,ticker,interval='1d'):
        try:
            df = yf.download(ticker,interval=interval,period='5y',auto_adjust=True,progress=False)
            df.columns = ['Open','High','Low','Close','Volume']
            df.index.name = 'Datetime'
            df.reset_index(inplace=True,drop=False)
            self._store(ticker,'ohlcv.db',df)
            return df
        except Exception as e:
            self.logger.error('Invalid Ticker',e)
        
    def get_fundamentals(self,ticker,freq='quarterly'):
        try:
            t = yf.Ticker(ticker)
            income = t.get_income_stmt(freq=freq)
            balance = t.get_balance_sheet(freq=freq)
            cashflow = t.get_cash_flow(freq=freq)
            df = pd.concat([income,balance,cashflow],join='outer').T
            if freq == 'quarterly':
                df.index = df.index.to_period('Q')
            else:
                df.index = df.index.year
            df.sort_index(inplace=True)
            df.index = df.index.astype('str')
            df.index.name = 'Datetime'
            y = df.copy().T
            df.reset_index(inplace=True,drop=False)
            self._store(ticker,'fundamentals.db',df)
            return y
        except Exception as e:
            self.logger.error('Invalid Ticker')
              
    def get_macroeconomics(self,indicator):
        try:
            with open('lists/configs.json','r') as f:
                key = json.load(f)['FRED_API_KEY']
        except (FileNotFoundError,KeyError):
            self.logger.error('Error retrieving FRED API-Key')
            return
            
        fred = Fred(key)
        
        try:
            df = pd.DataFrame(fred.get_series(indicator),columns=[indicator])
            df.index.name = 'Datetime'
            df.reset_index(inplace=True,drop=False)
            self._store(indicator,'macroeconomics.db',df)
            return df
        except Exception as e:
            self.logger.error('Invalid economic indicator\n',e)