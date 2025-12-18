"""
ETL Pipeline: API IPMA -> Spark -> Parquet
------------------------------------------
Extrai previsão do tempo de cidades portuguesas via API pública,
armazena o dado bruto (Raw) e processa com PySpark (Trusted).

Autor: Nicolas Martins
API: IPMA (api.ipma.pt)
"""

import os
import sys
import json
import logging
import requests
import socketserver
from datetime import datetime

# --- WINDOWS COMPATIBILITY PATCH ---
if sys.platform == 'win32':
    class MockUnixSocketServer(socketserver.TCPServer):
        pass
    socketserver.UnixStreamServer = MockUnixSocketServer
    socketserver.UnixDatagramServer = MockUnixSocketServer

# --- IMPORTAÇÕES DO SPARK ---
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, current_timestamp, lit, explode, current_date

# --- CONFIGURAÇÃO DE LOGS ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CAMINHOS DINÂMICOS ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
RAW_DIR = os.path.join(DATA_DIR, "raw")
PROCESSED_DIR = os.path.join(DATA_DIR, "processed", "clima_parquet")

# URL da API do IPMA (Previsão diária - Dia 0 / Hoje)
API_URL = "https://api.ipma.pt/open-data/forecast/meteorology/cities/daily/hp-daily-forecast-day0.json"

def get_spark_session():
    """Inicializa a Spark Session com suporte a Windows."""
    builder = SparkSession.builder \
        .appName("etl_clima_portugal") \
        .config("spark.sql.warehouse.dir", os.path.join(BASE_DIR, "spark-warehouse"))
    
    if sys.platform == 'win32':
        # Ajuste para o seu caminho do Hadoop
        os.environ["HADOOP_HOME"] = r"C:\hadoop"
        os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"
        builder = builder.config("spark.hadoop.fs.permissions.umask-mode", "000")

    return builder.getOrCreate()

def extract_from_api() -> str:
    """Etapa 1: Extract - Baixa JSON da API."""
    logger.info(f"Conectando à API: {API_URL}")
    try:
        response = requests.get(API_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        
        today_str = datetime.now().strftime("%Y-%m-%d")
        file_name = f"clima_{today_str}.json"
        
        os.makedirs(RAW_DIR, exist_ok=True)
        file_path = os.path.join(RAW_DIR, file_name)
        
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f)
            
        logger.info(f"Dados brutos salvos: {file_path}")
        return file_path
    except requests.exceptions.RequestException as e:
        logger.critical(f"Erro ao acessar API: {e}")
        sys.exit(1)

def transform_data(spark: SparkSession, input_path: str) -> DataFrame:
    """Etapa 2: Transform - Processa JSON e trata Schema."""
    logger.info("Iniciando processamento com Spark...")
    
    # Leitura do JSON Bruto
    df_raw = spark.read.option("multiline", "true").json(input_path)
    
    # Explode o array 'data' para ter uma linha por cidade
    df_exploded = df_raw.select(explode(col("data")).alias("previsao"))
    
    # Seleção de Colunas
    # NOTA: O campo forecastDate foi removido da API no day0, 
    # então usamos current_date() pois o endpoint refere-se ao dia atual.
    df_final = df_exploded.select(
        col("previsao.globalIdLocal").alias("id_cidade").cast("integer"),
        col("previsao.tMin").alias("temperatura_min").cast("double"),
        col("previsao.tMax").alias("temperatura_max").cast("double"),
        col("previsao.precipitaProb").alias("prob_chuva").cast("double"),
        col("previsao.predWindDir").alias("direcao_vento"),
        col("previsao.classWindSpeed").alias("velocidade_vento_classe").cast("integer"),
        # Coluna gerada via Spark, já que a API não forneceu
        current_date().alias("data_previsao")
    )

    # Enriquecimento
    df_final = df_final.withColumn("data_ingestao", current_timestamp())
    
    # Limpeza
    df_final = df_final.na.drop(subset=["id_cidade"])
    
    logger.info(f"Registros processados: {df_final.count()}")
    return df_final

def load_data(df: DataFrame, output_path: str):
    """Etapa 3: Load - Salva em Parquet."""
    logger.info(f"Salvando em: {output_path}")
    df.write \
        .mode("overwrite") \
        .partitionBy("data_previsao") \
        .parquet(output_path)
    logger.info("Pipeline finalizado com sucesso.")

def main():
    json_path = extract_from_api()
    spark = get_spark_session()
    try:
        df_clean = transform_data(spark, json_path)
        print("\n--- AMOSTRA DOS DADOS ---")
        df_clean.show(5, truncate=False)
        load_data(df_clean, PROCESSED_DIR)
    except Exception as e:
        logger.error(f"Erro no pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()