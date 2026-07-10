import os
import json
import duckdb
import tempfile
import requests
import pandas as pd
from datetime import datetime
from io import BytesIO
from pathlib import Path
from prefect import task
from utils.supabase_client import get_supabase_client

URL = "https://api.open-meteo.com/v1/forecast"
DB_PATH = Path(__file__).resolve().parent.parent / "Data" / "weather.duckdb"

@task(retries=3, retry_delay_seconds=10)
def extract_weather_data():
    response = requests.get(
        URL,         
        params={
            "latitude": -22.9068,
            "longitude": -43.1729,
            "daily": "temperature_2m_max,temperature_2m_min",
            "forecast_days": 7,
            "timezone": "America/Sao_Paulo"
        }, 
        timeout=30
    )
    response.raise_for_status()
    return response.json()

@task(retries=3, retry_delay_seconds=10)
def upload_raw_data(data):
    supabase = get_supabase_client()
    now = datetime.utcnow()

    file_path = (
        f'weather/raw/'
        f'year={now.year}/month={now.month:02d}/day={now.day:02d}/'
        f'weather_{now.strftime("%Y%m%d_%H%M%S")}.json'
    )

    json_bytes = json.dumps(data, ensure_ascii=False, indent=2).encode('utf-8')

    supabase.storage.from_('weather-data').upload(
        path=file_path,
        file=json_bytes,
        file_options={'content-type': 'application/json'}
    )
    return file_path

@task(retries=3, retry_delay_seconds=10)
def transform_weather_data(data):
    daily = data['daily']

    df = pd.DataFrame({
        'date': daily['time'],
        'temp_max': daily['temperature_2m_max'],
        'temp_min': daily['temperature_2m_min']
    })

    df['temp_range'] = df['temp_max'] - df['temp_min']
    df['city'] = 'Rio de Janeiro'
    df['ingestion_date'] = datetime.utcnow()
    return df

@task(retries=3, retry_delay_seconds=10)
def save_parquet(df):
    supabase = get_supabase_client()
    now = datetime.utcnow()

    file_path = (
        f'weather/processed/'
        f'year={now.year}/month={now.month:02d}/day={now.day:02d}/'
        f'weather_{now.strftime("%Y%m%d_%H%M%S")}.parquet'
    )

    parquet_buffer = BytesIO()
    df.to_parquet(parquet_buffer, engine='pyarrow', index=False)
    parquet_buffer.seek(0)

    supabase.storage.from_('weather-data').upload(
        path=file_path,
        file=parquet_buffer.getvalue(),
        file_options={'content-type': 'application/octet-stream'}
    )
    return file_path

@task(retries=3, retry_delay_seconds=10)
def load_to_duck(path_parquet):
    supabase = get_supabase_client()

    parquet_bytes = supabase.storage.from_('weather-data').download(path_parquet)

    with tempfile.NamedTemporaryFile(suffix='.parquet') as tmp:
        tmp.write(parquet_bytes)
        tmp.flush()
    
        os.makedirs(DB_PATH.parent, exist_ok=True)
        conn = duckdb.connect(str(DB_PATH))

        conn.execute("""
            CREATE TABLE IF NOT EXISTS weather_daily(
                date DATE,
                temp_max DOUBLE,
                temp_min DOUBLE,
                temp_range DOUBLE,
                city VARCHAR(40),
                ingestion_date DATE
            )
        """)
    
        conn.execute("""
            INSERT INTO weather_daily
            SELECT * FROM read_parquet(?)
        """, [tmp.name])
    
        conn.close()

@task(retries=3, retry_delay_seconds=10)
def create_analytics():
    os.makedirs(DB_PATH.parent, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))

    conn.execute("""
    CREATE OR REPLACE VIEW weather_summary AS
    SELECT
        city,
        AVG(temp_max) AS avg_temp_max,
        AVG(temp_min) AS avg_temp_min,
        AVG(temp_range) AS avg_temp_range
    FROM weather_daily
    GROUP BY city
    """)
    conn.close()