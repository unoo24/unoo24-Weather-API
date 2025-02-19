from etl_module.connectors.weather_api import WeatherApiClient
from etl_module.connectors.mysql import MySqlClient
import pandas as pd
from sqlalchemy import MetaData, Table, Column, String, DateTime, Integer, Float


def extract_weather(
    weather_api_client: WeatherApiClient, cities: list = ["Seoul", "Busan"]
) -> pd.DataFrame:
    """
    여러 도시의 날씨 데이터를 추출합니다.

    Parameters:
    - weather_api_client (WeatherApiClient): API에서 날씨 데이터를 가져오기 위한 클라이언트.

    Returns:
    - pd.DataFrame: 지정된 도시들의 날씨 데이터를 포함하는 DataFrame.
    """
    cities = ["Busan", "Seoul"]
    weather_data = []
    for city_name in cities:
        weather_data.append(weather_api_client.get_city(city_name=city_name))
    df = pd.json_normalize(weather_data)
    return df


def transform_weather(df: pd.DataFrame) -> pd.DataFrame:
    """
    날씨 데이터를 변환하고 전처리합니다.

    Parameters:
    - df (pd.DataFrame): 원본 날씨 데이터를 포함하는 DataFrame.

    Returns:
    - pd.DataFrame: 선택된 컬럼과 이름이 변경된 데이터로 구성된 변환된 DataFrame.
    """
    df["measured_at"] = pd.to_datetime(df["dt"], unit="s") + pd.Timedelta(
        hours=9
    )  # 한국시간
    df["dt"] = df["measured_at"].dt.strftime("%Y%m%d")  # 기준년월일 (YYYYMMDD)
    df["time"] = df["measured_at"].dt.strftime("%H%M%S")  # 기준년월일 (HHHHMMSS)
    df_selected = df[
        [
            "dt",
            "time",
            "measured_at",
            "id",
            "name",
            "main.temp",
            "main.humidity",
            "wind.speed",
        ]
    ]
    df_selected = df_selected.rename(  # 컬럼명 수정
        columns={
            "name": "city",
            "main.temp": "temperature",
            "main.humidity": "humidity",
            "wind.speed": "wind_speed",
        }
    )
    return df_selected


def load_weather(
    df: pd.DataFrame,
    my_sql_client: MySqlClient,
    method: str = "upsert",
) -> None:
    """
    변환된 날씨 데이터를 MySQL에 로드하는 함수.

    Parameters:
    - df (pd.DataFrame): 변환된 데이터
    - my_sql_client (MySqlClient): 데이터베이스 클라이언트
    - method (str, optional): 삽입 방법 ('insert', 'upsert', 'overwrite')
    """
    metadata = MetaData()
    table = Table(
        "daily_weather",
        metadata,
        Column("dt", String(8), nullable=False, primary_key=True),
        Column("time", String(6), nullable=False, primary_key=True),
        Column("measured_at", DateTime, nullable=False),
        Column("id", Integer, primary_key=True),
        Column("city", String(100), nullable=True),
        Column("temperature", Float, nullable=True),
        Column("humidity", Integer, nullable=True),
        Column("wind_speed", Float, nullable=True),
    )
    if method == "insert":
        my_sql_client.insert(df=df, table=table, metadata=metadata)
    elif method == "upsert":
        my_sql_client.upsert(df=df, table=table, metadata=metadata)
    elif method == "overwrite":
        my_sql_client.overwrite(df=df, table=table, metadata=metadata)
    else:
        raise Exception("올바른 method를 설정해주세요: [insert, upsert, overwrite]")
