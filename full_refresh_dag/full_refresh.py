
# import libraries
import json
import requests
import pandas as pd

from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import timedelta
from datetime import datetime
from zoneinfo import ZoneInfo

# Da Nang, Vietnam coordinates
LATITUDE = Variable.get("LATITUDE")
LONGITUDE = Variable.get("LONGITUDE")


def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="snowflake_default")
    return hook.get_conn().cursor()


# start of the DAG

@task
def extract(url):
    f = requests.get(url)
    f.raise_for_status()
    return (f.text)

@task
def transform(weather_raw):

    data = json.loads(weather_raw)

    # Convert data into DataFrame
    df = pd.DataFrame({
        "latitude": LATITUDE,
        "longitude": LONGITUDE,
        "date": data["daily"]["time"],
        "temp_max": data["daily"]["temperature_2m_max"],
        "temp_min": data["daily"]["temperature_2m_min"],
        "precipitation": data["daily"]["precipitation_sum"],
        "weather_code": data["daily"]["weather_code"]
    })

    df["date"] = pd.to_datetime(df["date"])

    return df


@task(task_id="full_refresh_load_to_snowflake")
def load(df, target_table):
    con = return_snowflake_conn()
    try:
        con.execute("BEGIN;")
        con.execute(f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
          latitude      DECIMAL(9,6)  NOT NULL,
          longitude     DECIMAL(9,6)  NOT NULL,
          date          DATE          NOT NULL,
          temp_max      DECIMAL(4,1),
          temp_min      DECIMAL(4,1),
          precipitation DECIMAL(4,1),
          weather_code  DECIMAL(4,1),
          PRIMARY KEY (latitude, longitude, date)
        )""")

        con.execute(f"DELETE FROM {target_table}")

        sql = f"""
            INSERT INTO {target_table}
                (latitude, longitude, date, temp_max, temp_min, precipitation, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        load_df = df.copy()
        load_df["date"] = load_df["date"].dt.strftime("%Y-%m-%d")
        load_df = load_df.astype(object).where(load_df.notna(), None)
        records = load_df.values.tolist()
        con.executemany(sql, records)
        print(f"Loaded {len(records)} records into {target_table}")

        con.execute("COMMIT;")

    except Exception as e:
        con.execute("ROLLBACK;")
        print(f"Load failed: {e}")
        raise e

with DAG(
    dag_id = 'WeatherDataFullRefresh',
    start_date = datetime(2026, 9, 15, tzinfo=ZoneInfo("America/Los_Angeles")),
    catchup=False,
    tags=['ETL'],
    schedule = '0 8 * * *'
) as dag:
    target_table = "demo_db.raw.weather_data_hw"
    url = Variable.get("weather_data_url")

    weather_raw = extract(url)
    df = transform(weather_raw)
    load(df, target_table)
