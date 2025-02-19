import os
from datetime import datetime
from dotenv import load_dotenv
from etl_module.connectors.weather_api import WeatherApiClient
from etl_module.connectors.mysql import MySqlClient
from etl_module.assets.weather import extract_weather, transform_weather, load_weather
from loguru import logger
import schedule
import yaml
import time

# pip install schedule
# pip install pyyaml
# pip install loguru


def main(config):
    """
    main 함수는 전체 ETL 프로세스를 실행합니다.

    1. 환경 변수를 로드하여 Weather API와 MySQL 데이터베이스 연결에 필요한 정보를 가져옵니다.
    2. WeatherApiClient와 MySqlClient 객체를 생성합니다.
    3. ETL 프로세스를 순차적으로 수행합니다:
       - 데이터를 Weather API에서 추출합니다.
       - 추출된 데이터를 변환하여 필요한 형태로 가공합니다.
       - 가공된 데이터를 MySQL 데이터베이스에 적재합니다.
    """
    # 현재 날짜와 시간을 기반으로 Log 파일명 생성
    current_datetime = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_filename = f"{config.get('log_folder_path')}/etl_process_{current_datetime}.log"
    logger.add(log_filename)

    logger.info("ETL 프로세스를 시작합니다.")
    load_dotenv()

    try:

        API_KEY = os.environ.get("API_KEY")
        DB_SERVER_HOST = os.environ.get("DB_SERVER_HOST")
        DB_USERNAME = os.environ.get("DB_USERNAME")
        DB_PASSWORD = os.environ.get("DB_PASSWORD")
        DB_DATABASE = os.environ.get("DB_DATABASE")
        DB_PORT = os.environ.get("DB_PORT")

        if not all(
            [API_KEY, DB_SERVER_HOST, DB_USERNAME, DB_PASSWORD, DB_DATABASE, DB_PORT]
        ):
            # 누락된 변수들 확인
            missing_vars = [
                var
                for var, value in [
                    ("API_KEY", API_KEY),
                    ("DB_SERVER_HOST", DB_SERVER_HOST),
                    ("DB_USERNAME", DB_USERNAME),
                    ("DB_PASSWORD", DB_PASSWORD),
                    ("DB_DATABASE", DB_DATABASE),
                    ("DB_PORT", DB_PORT),
                ]
                if value is None
            ]
            error_message = f"누락된 환경 변수: {', '.join(missing_vars)}"
            logger.error(error_message)
            raise ValueError(error_message)  # 누락된 환경 변수가 있으면 예외를 발생시킴

        logger.info("환경 변수를 성공적으로 로드했습니다.")

        weather_api_client = WeatherApiClient(api_key=API_KEY)
        logger.info("WeatherApiClient가 초기화되었습니다.")

        my_sql_client = MySqlClient(
            server_name=DB_SERVER_HOST,
            database_name=DB_DATABASE,
            username=DB_USERNAME,
            password=DB_PASSWORD,
            port=DB_PORT,
        )
        logger.info("MySqlClient가 초기화되었습니다.")

        # ETL 실행
        logger.info("Weather API에서 데이터 추출을 시작합니다.")
        df = extract_weather(
            weather_api_client=weather_api_client, cities=config.get("cities")
        )
        logger.info(
            f"데이터 추출이 완료되었습니다. 총 {len(df)}개의 레코드가 있습니다."
        )

        logger.info("데이터 변환을 시작합니다.")
        clean_df = transform_weather(df)
        logger.info(
            f"데이터 변환이 완료되었습니다. 변환된 데이터프레임의 크기: {clean_df.shape}"
        )

        logger.info("MySQL 데이터베이스로 데이터 적재를 시작합니다.")
        load_weather(df=clean_df, my_sql_client=my_sql_client)
        logger.info("데이터 적재가 성공적으로 완료되었습니다.")

    except Exception as e:
        logger.error(f"ETL 프로세스 중 오류가 발생했습니다. 오류: {e}")


if __name__ == "__main__":

    # get config variables
    yaml_file_path = __file__.replace(".py", ".yaml")
    print(f"YAML 파일 위치: {yaml_file_path}")

    with open(yaml_file_path) as yaml_file:
        config = yaml.safe_load(yaml_file)

    log_folder_path = config.get("log_folder_path")
    os.makedirs(log_folder_path, exist_ok=True)

    # 스케줄러 생성
    schedule.every(config.get("run_minutes")).minutes.do(main, config=config)

    while True:
        schedule.run_pending()
        time.sleep(5)
