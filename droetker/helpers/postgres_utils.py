from dataclasses import dataclass, field

import sqlalchemy
from sqlalchemy.sql import text
from sqlalchemy import create_engine, engine
import pandas as pd


@dataclass
class Postgres:
    user_id: str
    password: str
    host: str
    port: str
    db: str
    db_engine: engine = field(init=False)

    def __post_init__(self):
        self.db_engine = create_engine(f'postgresql://{self.user_id}:{self.password}@{self.host}:{self.port}/{self.db}')

    def insert_into_db(self, data: pd.DataFrame, table_name: str):
        print(self.db_engine)
        data.to_sql(table_name, self.db_engine, if_exists='append', index=False)
        # self.create_main_view(table_name=table_name)

    # def create_main_view(self, table_name: str):
    #     with self.db_engine.connect() as con:
    #         file = open(f"sql/create_view_{table_name}.sql")
    #         query = text(file.read())
    #         con.execute(query)

    def exec_sql_file(self, sql_file: str):
        with self.db_engine.connect() as con:
            file = open(f"sql/{sql_file}")
            query = text(file.read())
            # print(query)
            res = con.execute(query)
            print(res)

    def exec_query(self, sql_query):
        try:
            sql_query = sqlalchemy.text(sql_query)
            print(f"query : {sql_query}")
            with self.db_engine.connect() as con:
                res = con.execute(sql_query)

        except Exception as err:
            print(err)
            raise f'Query Exec Error : {err}'
        return res
