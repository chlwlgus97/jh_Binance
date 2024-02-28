from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime, timedelta
import time
import ccxt

# 로깅 설정
logging.basicConfig(level=logging.INFO)

# Binance API 키와 시크릿 설정
api_key = '4dqvjqCSUXkDmZWo0nQLCKGz0EqSHQFcXQwqGJaWyQyeUScerGXoweMFCKaMd6di' 
api_secret = '1pOOpOOpHaDqhxy0IKUQ67NPkfcSSDWsZUl9xpGiEUyxYI11YqO3jXBVmHwGTsVh'

# 데이터베이스 연결 정보
db_connection_string = "mysql+pymysql://zero:zero4321@35.216.66.247:3306/wpDB"

# Binance 클라이언트 인스턴스화
client = Client(api_key, api_secret)

# cctx를 사용해서 선물거래 계좌 객체 생성
exchange = ccxt.binance({
    'apiKey': api_key,
    'secret': api_secret,
    'enableRateLimit': True,
    'options': {'defaultType': 'future'}
})

def fetch_and_store_data():
    try:
        # 계좌 정보 가져오기
        account_info = client.futures_account()

        # assets와 positions 정보를 데이터프레임으로 변환
        assets_df = pd.DataFrame(account_info['assets'])
        positions_df = pd.DataFrame(account_info['positions'])

        # 필터링
        assets_df = assets_df[assets_df['walletBalance'].astype(float) > 0]
        positions_df = positions_df[positions_df['entryPrice'].astype(float) > 0]
        # Long/Short 포지션 타입 추가
        positions_df['positionType'] = positions_df['positionAmt'].astype(float).apply(lambda x: 'Long' if x > 0 else 'Short' if x < 0 else 'Flat')
        
        # 현재 시간
        current_time = datetime.now()
        assets_df['updated_at'] = current_time
        positions_df['updated_at'] = current_time
        
        # 데이터베이스에 저장
        engine = create_engine(db_connection_string)
        assets_df.to_sql('filtered_assets', con=engine, if_exists='append', index=False, method='multi', chunksize=10000)
        positions_df.to_sql('filtered_positions', con=engine, if_exists='append', index=False, method='multi', chunksize=10000)

        logging.info("Data successfully stored in the database.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

def fetch_and_store_pnl():
    try:
        # 심볼별 PNL 계산
        symbols = ["XRP/USDT", "BTC/USDT", "ETH/USDT"]
        since = 1708659300000  # 기준 시간 설정
        total_realized_pnl = 0

        for symbol in symbols:
            trades = exchange.fetchMyTrades(symbol=symbol, since=since)
            realized_pnl = sum(float(trade['info']['realizedPnl']) for trade in trades)
            total_realized_pnl += realized_pnl

        # 현재 시간
        current_time = datetime.now()

        # 총 PNL을 데이터베이스에 저장
        engine = create_engine(db_connection_string)
        pnl_df = pd.DataFrame({
            'total_pnl': [total_realized_pnl],
            'updated_at': [current_time]
        })
        pnl_df.to_sql('daily_pnl', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)

        logging.info("PNL data successfully stored in the database.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# 실행 함수
while True:
    fetch_and_store_data()  # 계좌 정보 수집 및 저장
    fetch_and_store_pnl()  # PNL 계산 및 저장
    print('Run Complete')
    time.sleep(5)  # 1초 간격으로 실행
