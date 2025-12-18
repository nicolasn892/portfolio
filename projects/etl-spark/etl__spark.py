"""
Exemplo de pipeline usando PySpark.
Objetivo: consumir um CSV, transformar, e salvar em parquet.
"""
import socketserver
import os
import sys

# --- DEFINIÇÃO DA VARIÁVEL QUE ESTAVA A FALTAR ---
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Força o caminho do Hadoop para o Spark
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["PATH"] += os.pathsep + r"C:\hadoop\bin"

# Simula os atributos de Unix que não existem no Windows
if not hasattr(socketserver, "UnixStreamServer"):
    class MockUnixServer(socketserver.TCPServer):
        pass
    socketserver.UnixStreamServer = MockUnixServer
    socketserver.UnixDatagramServer = MockUnixServer

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main():
    # Adicionada configuração para evitar conflitos de permissão no Windows
    spark = SparkSession.builder \
        .appName("etl_exemplo_portfolio") \
        .config("spark.sql.warehouse.dir", os.path.join(BASE_DIR, "spark-warehouse")) \
        .config("spark.hadoop.fs.permissions.umask-mode", "000") \
        .getOrCreate()

    # Caminho de entrada (Raw Data)
    input_path = r"C:\Users\Ana Claudia\Desktop\portfolio\projects\turismo_portugal\data\Turismo.csv"              

    # Leitura do CSV
    df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)

    # Transformações
    df = df.withColumn("ano", col("ano").cast("int")) \
           .withColumn("turistas_estrangeiros", col("turistas_estrangeiros").cast("long")) \
           .withColumn("turistas_nacionais", col("turistas_nacionais").cast("long")) \
           .withColumn("gasto_medio", col("gasto_medio").cast("double"))

    # Criar coluna total_turistas
    df = df.withColumn("total_turistas", col("turistas_estrangeiros") + col("turistas_nacionais"))

    # Remover duplicados
    df = df.dropDuplicates(["ano", "cidade"])

    # --- CAMINHO DE SAÍDA ---
    # Agora o BASE_DIR já está definido no topo do script
    output_path = os.path.join(BASE_DIR, "data", "processed", "turismo_parquet")
    
    # Gravação em Parquet
    df.write.mode("overwrite").partitionBy("ano").parquet(output_path)

    print(f"\nETL PySpark concluído com sucesso em 2025!")
    print(f"Dados guardados em: {output_path}")
    
    spark.stop()

if __name__ == "__main__":
    main()
