from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO)

# Binance API 키와 시크릿 설정
# zero_api
api_key = '4dqvjqCSUXkDmZWo0nQLCKGz0EqSHQFcXQwqGJaWyQyeUScerGXoweMFCKaMd6di' 
api_secret = '1pOOpOOpHaDqhxy0IKUQ67NPkfcSSDWsZUl9xpGiEUyxYI11YqO3jXBVmHwGTsVh'

# jh_api
# api_key = "nz3BiMJaQubfXwLFAT5z9hCb8RaBj6ec0ddNq88dhI63ADawKrnebrRfE8xtAW7j"
# api_secret = "rbTzXhZSllDqI2kfHgYL4IrfYcWHm3tFLHnWMDItMuIQYy3pVLzjHBaouaFoW3wX"

# 데이터베이스 연결 정보
db_connection_string = "mysql+pymysql://zero:zero4321@35.216.66.247:3306/wpDB"

# Binance 클라이언트 인스턴스화
client = Client(api_key, api_secret)

def fetch_and_store_data():
    try:
        # 계좌 정보 가져오기
        account_info = client.futures_account()

        # assets와 positions 정보를 데이터프레임으로 변환
        assets_df = pd.DataFrame(account_info['assets'])
        positions_df = pd.DataFrame(account_info['positions'])

        # 필터링
        assets_df = assets_df[assets_df['walletBalance'].astype(float) > 0] # walletBalance : 지갑 잔액 -> 자산이 보유 됐을때 정보가 뜨게끔
        positions_df = positions_df[positions_df['entryPrice'].astype(float) > 0] # entryPrice : 진입 가격 -> 매수/매도시 진입 가격으로 포지션 정보가 뜨게끔

        # 데이터베이스에 저장
        engine = create_engine(db_connection_string)
        assets_df.to_sql('filtered_assets', con=engine, if_exists='append', index=False, method='multi', chunksize=10000)
        positions_df.to_sql('filtered_positions', con=engine, if_exists='append', index=False, method='multi', chunksize=10000)

        logging.info("Data successfully stored in the database.")
    except Exception as e:
        
        logging.error(f"An error occurred: {e}")

# 실행 함수
while True:
    fetch_and_store_data()
    print('Run Complete')
    time.sleep(10)