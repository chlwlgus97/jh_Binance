from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
from sqlalchemy import create_engine, select, Table, MetaData
from sqlalchemy.orm import sessionmaker
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

# 데이터베이스 엔진 생성
engine = create_engine(db_connection_string)
metadata = MetaData()

# 데이터베이스에서 테이블 정의
assets_table = Table('filtered_assets', metadata, autoload_with=engine)
positions_table = Table('filtered_positions', metadata, autoload_with=engine)

# 세션 설정
Session = sessionmaker(bind=engine)

def upsert_dataframe(df, table, unique_columns):
    session = Session()
    try:
        for index, row in df.iterrows():
            stmt = select(table).where(
                *[table.c[col] == row[col] for col in unique_columns]
            )
            existing_row = session.execute(stmt).scalar_one_or_none()
            if existing_row:
                for col in unique_columns:
                    setattr(existing_row, col, row[col])
            else:
                new_record = table(**row.to_dict())
                session.add(new_record)
        session.commit()  # 변경사항 커밋
    except Exception as e:
        session.rollback()  # 오류 발생 시 롤백
        raise e
    finally:
        session.close()  # 세션 닫기

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

        # 데이터베이스에 upsert 실행
        upsert_dataframe(assets_df, assets_table, ['asset'])  # 'asset'을 고유 식별자로 가정
        upsert_dataframe(positions_df, positions_table, ['symbol'])  # 'symbol'을 고유 식별자로 가정

        logging.info("데이터베이스에 데이터가 성공적으로 저장되거나 업데이트되었습니다.")
    except Exception as e:
        logging.error(f"오류가 발생했습니다: {e}")

# 실행 함수
while True:
    fetch_and_store_data()
    print('실행 완료')
    time.sleep(5)  # 실행 주기를 5초로 변경
