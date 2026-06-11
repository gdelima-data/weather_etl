# Weather Data Pipeline (Open-Meteo + Prefect + Supabase + DuckDB)

## 📌 Visão Geral

Este projeto implementa um pipeline de engenharia de dados para ingestão, processamento e análise de dados meteorológicos utilizando a API da Open-Meteo.

O pipeline realiza:

* Extração de dados meteorológicos via API
* Armazenamento de dados brutos em nuvem (Supabase Storage)
* Transformação dos dados com Pandas
* Armazenamento em formato Parquet
* Carga dos dados em banco analítico local (DuckDB)
* Criação de base pronta para consumo em ferramentas de BI

---

## 🏗️ Arquitetura

```text
Open-Meteo API
      ↓
Prefect Orchestration
      ↓
Supabase Storage (RAW JSON)
      ↓
Pandas Transformation
      ↓
Supabase Storage (PARQUET)
      ↓
Parquet (Processed Layer)
      ↓
DuckDB 
      ↓
BI Tools
```

---

## ⚙️ Tecnologias Utilizadas

* Python 3.14.5
* Prefect (orquestração de workflows)
* Supabase Storage (cloud)
* Pandas (transformação de dados)
* PyArrow (serialização Parquet)
* DuckDB (engine analítica)
* Requests (consumo de API)

---

## 📊 Fonte de Dados

* Open-Meteo API
  https://open-meteo.com/

Dados utilizados:

* Temperatura máxima diária
* Temperatura mínima diária
* Janela de previsão: 7 dias
* Localização: Rio de Janeiro

---

## 🔄 Pipeline

### 1. Extração

Coleta dados meteorológicos da API Open-Meteo.

### 2. Armazenamento RAW

Salva JSON bruto no Supabase Storage com particionamento por data:

```text
weather/raw/year=YYYY/month=MM/day=DD/
```

---

### 3. Transformação

Processamento dos dados com Pandas:

* Normalização de estrutura JSON
* Criação de colunas derivadas:

  * temp_range
  * city
  * ingestion_date

---

### 4. Armazenamento Processado

Dados estruturados são salvos em formato Parquet:

```text
weather/processed/year=YYYY/month=MM/day=DD/
```

---

### 5. Camada Analítica (DuckDB)

Os dados Parquet são carregados em um banco DuckDB local para análises SQL.

---

## 📈 Exemplo de Análises

```sql
SELECT
    city,
    AVG(temp_max) AS avg_temp_max,
    AVG(temp_min) AS avg_temp_min,
    AVG(temp_range) AS avg_temp_range
FROM weather_daily
GROUP BY city;
```

---

## 📁 Estrutura do Projeto

```text
weather-pipeline/
│
├── weather_flow.py
├── requirements.txt
├── .env
│
├── data/
│   └── weather.duckdb
│
└── README.md
```

---

## 📌 Status do Projeto

✔ Extração de dados
✔ Armazenamento em nuvem (Supabase RAW)
✔ Transformação com Pandas
✔ Camada Processada (Parquet)
✔ Camada Analítica (DuckDB) - passível de melhoria futura
⏳ Dashboard (Metabase - futuro)

---

## 🔮 Próximos Passos

* Integração com ferramenta de BI para dashboards
* Separação em módulos (extract/transform/load)
* Deploy do Prefect em ambiente cloud
