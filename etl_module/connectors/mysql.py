from sqlalchemy import create_engine, MetaData, Table, MetaData, Column
from sqlalchemy import text
from sqlalchemy.engine import URL
import pandas as pd


class MySqlClient:
    """
    MySQL 데이터베이스와 상호작용하기 위한 클라이언트 클래스입니다.

    이 클래스는 SQLAlchemy를 사용하여 MySQL 데이터베이스에 연결하고 테이블을 생성, 삭제, 삽입,
    업서트(upsert)와 같은 작업을 지원합니다.
    """

    def __init__(
        self,
        server_name: str,
        database_name: str,
        username: str,
        password: str,
        port: int = 3306,
    ):
        # 데이터베이스 연결을 위한 초기 설정
        self.host_name = server_name
        self.database_name = database_name
        self.username = username
        self.password = password
        self.port = port

        # MySQL 연결 URL 생성
        connection_url = URL.create(
            drivername="mysql+mysqlconnector",  # 또는 mysql+pymysql
            username=username,
            password=password,
            host=server_name,
            port=port,
            database=database_name,
        )

        # SQLAlchemy 엔진 생성
        self.engine = create_engine(connection_url)

    def create_table(self, metadata: MetaData) -> None:
        """
        주어진 메타데이터 객체를 기반으로 테이블을 생성합니다.

        Parameters:
        - metadata (MetaData): 테이블 정의를 포함하는 SQLAlchemy MetaData 객체.
        """
        metadata.create_all(self.engine)

    def drop_table(self, table: Table) -> None:
        """
        지정된 테이블을 삭제합니다. 테이블이 존재하지 않으면 무시합니다.

        Parameters:
        - table_name (str): 삭제할 테이블의 이름.
        """
        with self.engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table.name}"))

    def insert(self, df: pd.DataFrame, table: Table, metadata: MetaData) -> None:
        """
        주어진 DataFrame을 테이블에 삽입합니다. 테이블이 존재하지 않으면 생성합니다.

        Parameters:
        - df (pd.DataFrame): 삽입할 데이터를 포함하는 Pandas DataFrame.
        - table (Table): 데이터 삽입을 위한 SQLAlchemy Table 객체.
        - metadata (MetaData): 테이블 정의를 포함하는 SQLAlchemy MetaData 객체.
        """
        self.create_table(metadata=metadata)
        df.to_sql(name=table.name, con=self.engine, if_exists="append", index=False)

    def upsert(self, df: pd.DataFrame, table: Table, metadata: MetaData) -> None:
        """
        주어진 DataFrame 데이터를 테이블에 삽입하고, 기존 레코드가 있으면 갱신합니다.

        Parameters:
        - df (pd.DataFrame): 삽입 또는 갱신할 데이터를 포함하는 Pandas DataFrame.
        - table (Table): 업서트 작업을 수행할 SQLAlchemy Table 객체.
        - metadata (MetaData): 테이블 정의를 포함하는 SQLAlchemy MetaData 객체.
        """
        self.create_table(metadata=metadata)

        # 데이터프레임을 레코드(딕셔너리 목록)으로 변환
        data = df.to_dict(orient="records")

        # 테이블의 고유 키(Primary Key) 추출
        key_columns = [
            pk_column.name for pk_column in table.primary_key.columns.values()
        ]
        key_values = [tuple(row[pk] for pk in key_columns) for row in data]
        delete_values = ", ".join(
            [f"({', '.join(map(repr, values))})" for values in key_values]
        )

        with self.engine.connect() as connection:
            if key_values:
                delete_sql = f"""
                    DELETE FROM {self.database_name}.{table.name}
                    WHERE ({', '.join(key_columns)}) IN (
                        {delete_values}
                    )
                """
                connection.execute(text(delete_sql))
                connection.commit()  # DELETE 문 실행

        # 변환된 데이터프레임을 테이블에 추가 (INSERT)
        df.to_sql(name=table.name, con=self.engine, if_exists="append", index=False)

    def overwrite(self, df: pd.DataFrame, table: Table, metadata: MetaData) -> None:
        """
        주어진 DataFrame 데이터를 테이블의 기존 데이터를 모두 대체하도록 삽입합니다.

        Parameters:
        - df (pd.DataFrame): 테이블의 기존 데이터를 대체할 데이터를 포함하는 Pandas DataFrame.
        - table (Table): 데이터를 대체할 SQLAlchemy Table 객체.
        - metadata (MetaData): 테이블 정의를 포함하는 SQLAlchemy MetaData 객체.
        """
        # 테이블이 존재하지 않으면 생성
        self.create_table(metadata=metadata)

        # 기존 테이블 데이터 삭제
        self.drop_table(table=table)

        # 새로운 데이터를 테이블에 삽입
        self.insert(df=df, table=table, metadata=metadata)
