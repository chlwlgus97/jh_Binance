from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
from sqlalchemy import create_engine, text
import logging
from datetime import datetime
import time

# 로깅 설정
logging.basicConfig(level=logging.INFO)

# Binance API 키와 시크릿 설정
api_key = '4dqvjqCSUXkDmZWo0nQLCKGz0EqSHQFcXQwqGJaWyQyeUScerGXoweMFCKaMd6di'
api_secret = '1pOOpOOpHaDqhxy0IKUQ67NPkfcSSDWsZUl9xpGiEUyxYI11YqO3jXBVmHwGTsVh'

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
        assets_df = assets_df[assets_df['walletBalance'].astype(float) > 0]  # 지갑 잔액이 있는 자산만 선택
        positions_df = positions_df[positions_df['entryPrice'].astype(float) > 0]  # 진입 가격이 있는 포지션만 선택

        # 현재 시각 추가
        assets_df['updated_at'] = datetime.now()
        positions_df['updated_at'] = datetime.now()
 
        # 데이터베이스 엔진 생성
        engine = create_engine(db_connection_string)
        with engine.connect() as conn:
            # assets 데이터베이스에 저장 또는 업데이트 쿼리 실행
            for index, row in assets_df.iterrows():
                conn.execute(text("""
                INSERT INTO filtered_assets (asset, walletBalance, updated_at)
                VALUES (:asset, :walletBalance, :updated_at)
                ON DUPLICATE KEY UPDATE
                walletBalance = VALUES(walletBalance),
                updated_at = VALUES(updated_at);
                """), row.to_dict())

            # positions 데이터베이스에 저장 또는 업데이트 쿼리 실행
            for index, row in positions_df.iterrows():
                conn.execute(text("""
                INSERT INTO filtered_positions (symbol, entryPrice, updated_at)
                VALUES (:symbol, :entryPrice, :updated_at)
                ON DUPLICATE KEY UPDATE
                entryPrice = VALUES(entryPrice),
                updated_at = VALUES(updated_at);
                """), row.to_dict())

        logging.info("Data successfully stored in the database.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# 실행 함수
if __name__ == "__main__":
    while True:
        fetch_and_store_data()
        logging.info('Run Complete')
        time.sleep(10) # 10초마다 실행
