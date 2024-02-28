from binance.client import Client
from binance.exceptions import BinanceAPIException
import pandas as pd
from sqlalchemy import create_engine
import logging
from datetime import datetime, timedelta
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

        # 오늘의 시작과 끝 시간을 UTC로 설정
        utc_now = datetime.utcnow()
        start_of_day = utc_now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + timedelta(days=1)

       # 오늘의 실현된 손익 정보 가져오기 및 처리
        today_pnl = client.futures_income_history(incomeType='REALIZED_PNL', startTime=int(start_of_day.timestamp() * 1000), endTime=int(end_of_day.timestamp() * 1000))
        # DataFrame 생성 전에 income 값에 부호 포함
        for pnl in today_pnl:
            # income 값을 float로 변환 (API로부터 문자열로 받을 수 있음)
            income_value = float(pnl['income'])
            # 부호가 분명하게 포함된 문자열로 변환
            pnl['income'] = f"{income_value:+.8f}"  # +를 포함하여 양수, -가 자동으로 포함되어 음수를 나타냄
            
        # 변환된 데이터를 dataFream으로 변환    
        pnl_df = pd.DataFrame(today_pnl)

        # 필터링 및 포지션 타입 추가
        assets_df = assets_df[assets_df['walletBalance'].astype(float) > 0]
        positions_df = positions_df[positions_df['entryPrice'].astype(float) > 0]
        positions_df['positionType'] = positions_df['positionAmt'].astype(float).apply(lambda x: 'Long' if x > 0 else ('Short' if x < 0 else 'Flat'))

        # 현재 시간
        current_time = datetime.now()
        assets_df['updated_at'] = current_time
        positions_df['updated_at'] = current_time
        pnl_df['updated_at'] = current_time  # 실현된 PnL 데이터에 현재 시간 추가

        # 데이터베이스에 저장
        engine = create_engine(db_connection_string)
        assets_df.to_sql('filtered_assets', con=engine, if_exists='append', index=False, method='multi', chunksize=10000)
        positions_df.to_sql('filtered_positions', con=engine, if_exists='append', index=False, method='multi', chunksize=10000)
        pnl_df.to_sql('today_realized_pnl', con=engine, if_exists='append', index=False, method='multi', chunksize=10000)  # 실현된 PnL 데이터 저장

        logging.info("Data successfully stored in the database.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")

# 실행 함수
while True:
    fetch_and_store_data()
    print('Run Complete') 
    time.sleep(5)  # 주의: 이는 데모 목적으로만 사용됩니다. 실제 사용 시 API 호출 제한을 고려해 적절한 대기 시간 설정이 필요합니다.
