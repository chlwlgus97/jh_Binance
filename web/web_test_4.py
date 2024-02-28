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

def update_or_insert_dataframe(engine, df, table_name):
    with engine.connect() as conn:
        for index, row in df.iterrows():
            if table_name == 'filtered_assets':
                key_column = 'asset'
            else:
                key_column = 'symbol'
            
            # 데이터가 존재하는지 확인
            select_stmt = text(f"SELECT COUNT(1) FROM {table_name} WHERE {key_column} = :key")
            result = conn.execute(select_stmt, {'key': row[key_column]}).fetchone()
            
            if result[0] > 0:
                # 데이터 업데이트
                update_stmt = text(f"""
                    UPDATE {table_name}
                    SET walletBalance = :walletBalance, updated_at = :updated_at
                    WHERE {key_column} = :key
                """)
            else:
                # 데이터 삽입
                update_stmt = text(f"""
                    INSERT INTO {table_name} ({key_column}, walletBalance, updated_at)
                    VALUES (:key, :walletBalance, :updated_at)
                """)
            conn.execute(update_stmt, {'key': row[key_column], 'walletBalance': row['walletBalance'], 'updated_at': datetime.now()})

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

        # 데이터베이스 엔진 생성
        engine = create_engine(db_connection_string)
        
        # 데이터 업데이트 또는 삽입
        update_or_insert_dataframe(engine, assets_df, 'filtered_assets')
        update_or_insert_dataframe(engine, positions_df, 'filtered_positions')

        logging.info("Data successfully stored or updated in the database.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# 실행 함수
if __name__ == "__main__":
    while True:
        fetch_and_store_data()
        logging.info('Run Complete')
        time.sleep(10)  # 적절한 sleep 시간 설정 필요
