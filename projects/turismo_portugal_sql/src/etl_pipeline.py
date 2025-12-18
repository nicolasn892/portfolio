"""
Pipeline de Data Warehousing - Turismo Portugal
-----------------------------------------------
1. Ingestão de dados reais do INE/Pordata (Excel).
2. Modelagem Dimensional (Star Schema) em SQLite.
3. Carga de dados (ETL).

Autor: Nicolas Martins
"""

import os
import pandas as pd
import sqlite3
import logging
import numpy as np
from datetime import datetime

# --- CONFIG ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# Garante que a pasta processed existe
os.makedirs(os.path.join(BASE_DIR, "data", "processed"), exist_ok=True)

DB_PATH = os.path.join(BASE_DIR, "data", "processed", "turismo_dw.db")
RAW_PATH = os.path.join(BASE_DIR, "data", "raw", "turismo_ine.xlsx")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger()

def create_connection():
    """Cria conexão com banco SQL local (SQLite)."""
    conn = sqlite3.connect(DB_PATH)
    return conn

def create_star_schema(conn):
    """
    Cria a estrutura de Tabelas (DDL) seguindo o modelo Star Schema.
    """
    cursor = conn.cursor()
    
    # 1. Dimensão Tempo
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_tempo (
        id_tempo INTEGER PRIMARY KEY,
        ano INTEGER,
        mes INTEGER,
        trimestre INTEGER
    );
    """)

    # 2. Dimensão Localização
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_localizacao (
        id_local INTEGER PRIMARY KEY AUTOINCREMENT,
        municipio TEXT,
        distrito TEXT,
        regiao TEXT
    );
    """)

    # 3. Tabela Fato
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fato_turismo (
        id_fato INTEGER PRIMARY KEY AUTOINCREMENT,
        id_tempo INTEGER,
        id_local INTEGER,
        total_hospedes INTEGER,
        proveito_total REAL,
        FOREIGN KEY(id_tempo) REFERENCES dim_tempo(id_tempo),
        FOREIGN KEY(id_local) REFERENCES dim_localizacao(id_local)
    );
    """)
    
    conn.commit()
    logger.info("Schema SQL (Star Schema) criado/verificado com sucesso.")

def extract_transform_data():
    """
    Lê o Excel ou gera dados de amostra se o arquivo não existir.
    """
    if os.path.exists(RAW_PATH):
        logger.info(f"Lendo arquivo real: {RAW_PATH}")
        df = pd.read_excel(RAW_PATH, header=0) 
    else:
        logger.warning("Arquivo 'turismo_ine.xlsx' não encontrado. Gerando dados reais simulados...")
        
        municipios = [
            ("Lisboa", "Lisboa", "AML"), ("Porto", "Porto", "Norte"), 
            ("Funchal", "Madeira", "Madeira"), ("Faro", "Faro", "Algarve"),
            ("Coimbra", "Coimbra", "Centro"), ("Braga", "Braga", "Norte"),
            ("Aveiro", "Aveiro", "Centro"), ("Cascais", "Lisboa", "AML")
        ]
        
        data = []
        for ano in [2023, 2024]:
            for mes in range(1, 13):
                for mun, dist, reg in municipios:
                    hospedes = int(np.random.randint(5000, 500000))
                    # Convertendo explicitamente para float Python
                    receita = float(hospedes * np.random.uniform(50.0, 150.0))
                    data.append([ano, mes, mun, dist, reg, hospedes, receita])
        
        df = pd.DataFrame(data, columns=["Ano", "Mes", "Municipio", "Distrito", "Regiao", "Hospedes", "Receita"])

    return df

def load_data(conn, df):
    """Carrega os dados transformados nas tabelas SQL."""
    cursor = conn.cursor()
    logger.info("Iniciando carga de dados...")

    # --- 1. Carga Dimensão Tempo ---
    df["id_tempo"] = df["Ano"] * 100 + df["Mes"]
    df["trimestre"] = ((df["Mes"] - 1) // 3) + 1
    
    dim_tempo = df[["id_tempo", "Ano", "Mes", "trimestre"]].drop_duplicates()
    
    for _, row in dim_tempo.iterrows():
        # A CORREÇÃO ESTÁ AQUI: Convertendo explicitamente para int()
        # O SQLite não aceita numpy.int64, precisa ser python int puro
        cursor.execute("INSERT OR IGNORE INTO dim_tempo VALUES (?, ?, ?, ?)", 
                       (int(row["id_tempo"]), int(row["Ano"]), int(row["Mes"]), int(row["trimestre"])))

    # --- 2. Carga Dimensão Localização ---
    locais_unicos = df[["Municipio", "Distrito", "Regiao"]].drop_duplicates()
    mapa_local_id = {}
    
    for _, row in locais_unicos.iterrows():
        cursor.execute("SELECT id_local FROM dim_localizacao WHERE municipio = ?", (row["Municipio"],))
        res = cursor.fetchone()
        
        if res:
            mapa_local_id[row["Municipio"]] = res[0]
        else:
            cursor.execute("INSERT INTO dim_localizacao (municipio, distrito, regiao) VALUES (?, ?, ?)", 
                           (row["Municipio"], row["Distrito"], row["Regiao"]))
            mapa_local_id[row["Municipio"]] = cursor.lastrowid

    # --- 3. Carga Tabela Fato ---
    count = 0
    for _, row in df.iterrows():
        id_local = mapa_local_id.get(row["Municipio"])
        
        # A CORREÇÃO ESTÁ AQUI TAMBÉM: Convertendo valores
        cursor.execute("""
        INSERT INTO fato_turismo (id_tempo, id_local, total_hospedes, proveito_total)
        VALUES (?, ?, ?, ?)
        """, (
            int(row["id_tempo"]), 
            int(id_local), 
            int(row["Hospedes"]), 
            float(row["Receita"]) # float para moeda/dinheiro
        ))
        count += 1

    conn.commit()
    logger.info(f"Carga concluída! {count} registros inseridos na Fato.")

def run_query_demo(conn):
    """Executa uma query SQL analítica para provar que funcionou."""
    logger.info("\n--- DEMONSTRAÇÃO DE ANÁLISE SQL (TOP 3 RECEITAS EM 2024) ---")
    query = """
    SELECT 
        l.municipio, 
        t.ano, 
        printf('€ %,.2f', SUM(f.proveito_total)) as receita_anual
    FROM fato_turismo f
    JOIN dim_localizacao l ON f.id_local = l.id_local
    JOIN dim_tempo t ON f.id_tempo = t.id_tempo
    WHERE t.ano = 2024
    GROUP BY l.municipio, t.ano
    ORDER BY SUM(f.proveito_total) DESC
    LIMIT 3;
    """
    try:
        # Usa pandas para exibir bonitinho no terminal
        df_result = pd.read_sql_query(query, conn)
        print(df_result.to_string(index=False))
    except Exception as e:
        print(f"Erro na exibição: {e}")
    print("-" * 50)

def main():
    conn = create_connection()
    try:
        create_star_schema(conn)
        df_raw = extract_transform_data()
        load_data(conn, df_raw)
        run_query_demo(conn)
        logger.info(f"Banco de dados disponível em: {DB_PATH}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()