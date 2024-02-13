import numpy as np
import pandas as pd 
import requests
from datetime import datetime
import time
from tqdm import tqdm

result = requests.get('https://api.binance.com/api/v3/ticker/price')
js = result.json()
symbols = [x['symbol'] for x in js]
symbols_usdt = [x for x in symbols if 'USDT' in x] 

COLUMNS = ['Open_time', 'Open', 'High', 'Low', 'Close', 'Volume', 'Close_time', 'quote_av', 'trades', 
            'tb_base_av', 'tb_quote_av', 'ignore']
URL = 'https://api.binance.com/api/v3/klines'
def get_data(start_date, end_date, symbol):
    data = []
    
    start = int(time.mktime(datetime.strptime(start_date + ' 00:00', '%Y-%m-%d %H:%M').timetuple())) * 1000
    end = int(time.mktime(datetime.strptime(end_date +' 23:59:59', '%Y-%m-%d %H:%M:%S').timetuple())) * 1000
    params = {
        'symbol': symbol,
        'interval': '1m',
        'limit': 1000,
        'startTime': start,
        'endTime': end
    }
    
    while start < end:
        print(datetime.fromtimestamp(start // 1000))
        params['startTime'] = start
        result = requests.get(URL, params = params)
        js = result.json()
        if not js:
            break
        data.extend(js)  # result에 저장
        start = js[-1][0] + 60000  # 다음 step으로
    # 전처리
    if not data:  # 해당 기간에 데이터가 없는 경우
        print('해당 기간에 일치하는 데이터가 없습니다.')
        return -1
    df = pd.DataFrame(data)
    df.columns = COLUMNS
    df['Open_time'] = df.apply(lambda x:datetime.fromtimestamp(x['Open_time'] // 1000), axis=1)
    df = df.drop(columns = ['Close_time', 'ignore'])
    df['Symbol'] = symbol
    df.loc[:, 'Open':'tb_quote_av'] = df.loc[:, 'Open':'tb_quote_av'].astype(float)  # string to float
    df['trades'] = df['trades'].astype(int)
    
    return df.to_csv('C:\education\ml-dev\project\BTC_csv\BTC_0114_1m.csv',  index=False)

start_date = '2024-1-14'
end_date = '2024-1-14'
symbol = symbols_usdt[0]
get_data(start_date, end_date, symbol)