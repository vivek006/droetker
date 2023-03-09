import requests as r
import re
import pandas as pd

DATA_URL = "https://storage.googleapis.com/datascience-public/data-eng-challenge/MOCK_DATA.json"

# regular expression for validating an IP
regex = "^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])$"

from droetker.helpers.postgres_utils import Postgres


def fetch_data_from_url():
    res = r.get(DATA_URL)
    return res.json()


def save_to_db():
    df = pd.read_csv('raw_data.csv')
    db_instance = Postgres(
        db='demo_data',
        user_id='postgres',
        password='postgres',
        port="5432",
        host="127.0.0.1",

    )
    db_instance.insert_into_db(data=df, table_name='stg_demo_data')


def extract_data():
    res_data = fetch_data_from_url()
    df = pd.DataFrame(res_data)
    df['is_valid_ip'] = df.apply(lambda x: 1 if re.search(regex, x.ip_address) else 0, axis=1)
    df['datetime'] = pd.to_datetime(df['date'], infer_datetime_format=True)
    df.to_csv('raw_data.csv', index=False)


if __name__ == '__main__':
    extract_data()
    save_to_db()

