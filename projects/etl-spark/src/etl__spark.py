"""
ETL Pipeline com PySpark
------------------------
Este script executa um pipeline completo (Extract, Transform, Load) 
para processamento de dados de turismo.

Autor: Nicolas Martins
Data: 2025
"""

import os
import sys
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, when

# --- CONFIGURAÇÃO DE LOGS (Essencial para Debug em Produção) ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DE AMBIENTE (Caminhos Relativos) ---
# Define a raiz do projeto baseada na localização deste script
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

def get_spark_session(app_name: str = "etl_turismo") -> SparkSession:
    """
    Inicializa e retorna uma sessão Spark.
    Trata configurações específicas de OS (Windows vs Linux) se necessário.
    """
    try:
        # Configuração para Windows (Apenas se necessário localmente)
        if sys.platform == 'win32':
            os.environ["HADOOP_HOME"] = os.path.join(BASE_DIR, "resources", "hadoop")
            # Adicione aqui os hacks de socketserver apenas se estritamente necessário para rodar local
            
        spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.warehouse.dir", os.path.join(BASE_DIR, "spark-warehouse")) \
            .getOrCreate()
            
        logger.info("Spark Session iniciada com sucesso.")
        return spark
    except Exception as e:
        logger.error(f"Erro ao iniciar Spark Session: {e}")
        sys.exit(1)

def extract_data(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Etapa de Extração: Lê o arquivo CSV bruto.
    """
    logger.info(f"Iniciando leitura do arquivo: {file_path}")
    try:
        df = spark.read \
            .option("header", True) \
            .option("inferSchema", True) \
            .option("delimiter", ",") \
            .csv(file_path)
        
        logger.info(f"Dados extraídos. Contagem de linhas: {df.count()}")
        return df
    except Exception as e:
        logger.error(f"Falha na extração dos dados: {e}")
        raise e

def transform_data(df: DataFrame) -> DataFrame:
    """
    Etapa de Transformação: Limpeza, tipagem e criação de novas colunas.
    """
    logger.info("Iniciando transformações...")
    
    # 1. Renomear colunas (Boas práticas: minúsculas e sem espaços)
    # Supondo que as colunas originais possam ter nomes diferentes, aqui garantimos o padrão
    # Exemplo: Se viesse "Ano", "Turistas Estrangeiros"
    
    # 2. Tipagem de dados (Casting explícito é mais seguro que inferSchema)
    df_transformed = df \
        .withColumn("ano", col("ano").cast("int")) \
        .withColumn("turistas_estrangeiros", col("turistas_estrangeiros").cast("long")) \
        .withColumn("turistas_nacionais", col("turistas_nacionais").cast("long")) \
        .withColumn("gasto_medio", col("gasto_medio").cast("double"))

    # 3. Regras de Negócio (Enrichment)
    # Tratamento de nulos (exemplo: substituir nulos por 0)
    df_transformed = df_transformed.fillna(0, subset=["turistas_estrangeiros", "turistas_nacionais"])

    # Criação de coluna calculada
    df_transformed = df_transformed.withColumn(
        "total_turistas", 
        col("turistas_estrangeiros") + col("turistas_nacionais")
    )

    # 4. Limpeza (Deduplicação)
    count_before = df_transformed.count()
    df_transformed = df_transformed.dropDuplicates(["ano", "cidade"])
    count_after = df_transformed.count()
    
    if count_before > count_after:
        logger.warning(f"Duplicatas removidas: {count_before - count_after} registros.")

    logger.info("Transformações concluídas.")
    return df_transformed

def load_data(df: DataFrame, output_path: str):
    """
    Etapa de Carregamento: Salva os dados processados em formato Parquet particionado.
    """
    logger.info(f"Salvando dados em: {output_path}")
    try:
        df.write \
            .mode("overwrite") \
            .partitionBy("ano") \
            .parquet(output_path)
        logger.info("Carga de dados concluída com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar os dados: {e}")
        raise e

def main():
    spark = get_spark_session()
    
    # Definição de caminhos RELATIVOS (Funciona em qualquer PC)
    input_file = os.path.join(DATA_DIR, "raw", "Turismo.csv")
    output_dir = os.path.join(DATA_DIR, "processed", "turismo_parquet")

    # Pipeline de Execução
    try:
        # 1. Extract
        df_raw = extract_data(spark, input_file)
        
        # 2. Transform
        df_clean = transform_data(df_raw)
        
        # 3. Load
        load_data(df_clean, output_dir)
        
        logger.info("Pipeline ETL finalizado com sucesso.")
        
    except Exception as e:
        logger.critical(f"Falha fatal no pipeline: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()