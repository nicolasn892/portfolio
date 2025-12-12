"""
Exemplo de pipeline PySpark (local/databricks).
Objetivo: consumir um JSON/CSV, transformar, e salvar em parquet.
Este é um script exemplo que mostra boas práticas e comentários.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

def main():
    spark = SparkSession.builder \
        .appName("etl_exemplo_portfolio") \
        .getOrCreate()

    # Leitura: substitua pelo caminho real (pode ser S3, ADLS, local, etc.)
    input_path = "s3a://meu-bucket/exemplo/input/*.csv"  # exemplo, ajuste conforme necessário
    # Para teste local, coloque um CSV em ../data/ e use "data/exemplo.csv"

    # EXEMPLO: leitura CSV
    df = spark.read.option("header", True).csv("../data/turismo.csv")

    # Transformação: garantir tipos corretos
    df = df.withColumn("ano", col("ano").cast("int")) \
           .withColumn("turistas_estrangeiros", col("turistas_estrangeiros").cast("long")) \
           .withColumn("turistas_nacionais", col("turistas_nacionais").cast("long")) \
           .withColumn("gasto_medio", col("gasto_medio").cast("double"))

    # Exemplo: criar coluna total_turistas
    from pyspark.sql.functions import expr
    df = df.withColumn("total_turistas", col("turistas_estrangeiros") + col("turistas_nacionais"))

    # Filtros, dedup, etc.
    df = df.dropDuplicates(["ano","cidade"])

    # Salvar em parquet particionado por ano (boa prática)
    output_path = "../data/processed/turismo_parquet"
    df.write.mode("overwrite").partitionBy("ano").parquet(output_path)

    print("ETL PySpark concluído. Output:", output_path)
    spark.stop()

if __name__ == "__main__":
    main()
