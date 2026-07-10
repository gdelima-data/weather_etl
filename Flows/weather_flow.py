import sys
from pathlib import Path
from prefect import flow

# Garante que o Prefect encontre a pasta raiz para os imports funcionarem
sys.path.append(str(Path(__file__).resolve().parent.parent))

from flow.tasks import (
    extract_weather_data,
    upload_raw_data,
    transform_weather_data,
    save_parquet,
    load_to_duck,
    create_analytics
)

@flow(name="Weather ETL Pipeline")
def weather_pipeline():
    # 1. Extração
    data = extract_weather_data()
    
    # 2. Carga do dado bruto
    file_path = upload_raw_data(data)

    # 3. Transformação
    data_transform = transform_weather_data(data)

    # 4. Carga do dado transformado
    path_parquet = save_parquet(data_transform)

    # 5. Armazenamento local e Analytics
    load_to_duck(path_parquet)
    create_analytics()

    print(f'Pipeline executado com sucesso!')
    print(f'Raw: {file_path} | Processed: {path_parquet}')

if __name__ == "__main__":
    weather_pipeline()