from download import Download
import time
import logging

if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR)
    file = 'lists/sp500.txt'
    
    with open(file,'r') as f:
        tickers = [i.strip() for i in f.readlines()]
    
    obj = Download()
    
    for i in tickers:
        time.sleep(3)
        obj.get_fundamentals(i)
        time.sleep(1)
        obj.get_ohlcv(i)