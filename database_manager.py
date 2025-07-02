import sqlite3
import pandas as pd
from sqlalchemy import create_engine

DATABASE_FILE = "cripto_data.db"


def get_sqlite_engine():
    return create_engine(f"sqlite:///{DATABASE_FILE}")


"""
    Cria as tabelas 'top_5_coins' e 'other_coins' no banco de dados SQLite,
    se elas não existirem.
    """


def create_tables(engine):
    print(f"Verificando/Criando tabelas no banco de dados: {DATABASE_FILE}...")
    try:
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS top_5_coins (
                    rank INTEGER PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    priceUsd REAL,
                    marketCapUsd REAL,
                    volumeUsd24Hr REAL,
                    changePercent24Hr REAL
                )
            """)
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS other_coins (
                    rank INTEGER PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    priceUsd REAL,
                    marketCapUsd REAL,
                    volumeUsd24Hr REAL,
                    changePercent24Hr REAL
                )
            """)
            conn.commit()
        print("Tabelas 'top_5_coins' e 'other_coins' verificadas/criadas com sucesso.")
    except sqlite3.Error as e:
        print(f"Erro ao criar tabelas: {e}")


"""
    Salva dois DataFrames do Pandas em tabelas separadas no banco de dados SQLite.
    As tabelas existentes são substituídas.
    """


def save_dataframes_to_sqlite(df_top_5: pd.DataFrame, df_others: pd.DataFrame):
    engine = get_sqlite_engine()

    print("Salvando dados das 5 primeiras moedas no SQLite...")
    try:
        df_top_5.to_sql("top_5_coins", con=engine, if_exists="replace", index=False)
        print("Dados das 5 primeiras moedas salvos em 'top_5_coins'.")
    except Exception as e:
        print(f"Erro ao salvar top_5_coins: {e}")

    print("Salvando dados das outras moedas no SQLite...")
    try:
        df_others.to_sql("other_coins", con=engine, if_exists="replace", index=False)
        print("Dados das outras moedas salvos em 'other_coins'.")
    except Exception as e:
        print(f"Erro ao salvar other_coins: {e}")


"""
    Busca dados de uma tabela específica no banco de dados SQLite e retorna como DataFrame.
    """


def fetch_data_from_sqlite(table_name: str):
    engine = get_sqlite_engine()
    print(f"Buscando dados da tabela '{table_name}' no SQLite...")
    try:
        df = pd.read_sql_table(table_name, con=engine)
        print(f"Dados da tabela '{table_name}' carregados com sucesso.")
        return df
    except Exception as e:
        print(f"Erro ao buscar dados da tabela '{table_name}': {e}")
        return pd.DataFrame()
