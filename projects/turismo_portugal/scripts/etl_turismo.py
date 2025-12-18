# etl_turismo.py
import pandas as pd
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DATA_IN = BASE / "data" / "turismo.csv"
DATA_OUT = BASE / "data" / "turismo_limpo.csv"

def run_etl():
    df = pd.read_csv(DATA_IN)
    # limpeza básica
    df = df.drop_duplicates()
    df = df.dropna()
    # transformação simples
    df['total_turistas'] = df['turistas_estrangeiros'] + df['turistas_nacionais']
    # ordenar por ano, cidade
    df = df.sort_values(['ano','cidade'])
    # salvar
    df.to_csv(DATA_OUT, index=False)
    print(f"ETL concluído: {DATA_OUT}")

if __name__ == "__main__":
    run_etl()
