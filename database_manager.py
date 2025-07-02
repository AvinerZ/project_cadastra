# database_manager.py
import sqlite3
import pandas as pd
from sqlalchemy import create_engine

DATABASE_FILE = "cripto_data.db" # Nome do arquivo do banco de dados SQLite

def get_sqlite_engine():
    """
    Retorna um SQLAlchemy engine para o banco de dados SQLite.
    """
    # create_engine cria uma conexão com o banco de dados.
    # O f-string "sqlite:///{DATABASE_FILE}" indica que é um banco SQLite local.
    return create_engine(f'sqlite:///{DATABASE_FILE}')

def create_tables(engine):
    """
    Cria as tabelas 'top_5_coins' e 'other_coins' no banco de dados SQLite,
    se elas não existirem.
    """
    print(f"Verificando/Criando tabelas no banco de dados: {DATABASE_FILE}...")
    try:
        # Apenas para garantir que as tabelas existem.
        # Pandas to_sql já cria a tabela se ela não existe, mas isso pode ser útil para verificação.
        with sqlite3.connect(DATABASE_FILE) as conn:
            cursor = conn.cursor()
            
            # Exemplo de criação de tabela manual (Pandas to_sql é mais conveniente)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS top_5_coins (
                    rank INTEGER PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    priceUsd REAL,
                    marketCapUsd REAL,
                    volumeUsd24Hr REAL,
                    changePercent24Hr REAL
                )
            ''')
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS other_coins (
                    rank INTEGER PRIMARY KEY,
                    symbol TEXT,
                    name TEXT,
                    priceUsd REAL,
                    marketCapUsd REAL,
                    volumeUsd24Hr REAL,
                    changePercent24Hr REAL
                )
            ''')
            conn.commit()
        print("Tabelas 'top_5_coins' e 'other_coins' verificadas/criadas com sucesso.")
    except sqlite3.Error as e:
        print(f"Erro ao criar tabelas: {e}")

def save_dataframes_to_sqlite(df_top_5: pd.DataFrame, df_others: pd.DataFrame):
    """
    Salva dois DataFrames do Pandas em tabelas separadas no banco de dados SQLite.
    """
    engine = get_sqlite_engine()
    
    print("Salvando dados das 5 primeiras moedas...")
    try:
        # 'replace' apaga a tabela existente e recria, 'append' adiciona dados
        # Vamos usar 'replace' para garantir dados atualizados a cada execução
        df_top_5.to_sql('top_5_coins', con=engine, if_exists='replace', index=False)
        print("Dados das 5 primeiras moedas salvos em 'top_5_coins'.")
    except Exception as e:
        print(f"Erro ao salvar top_5_coins: {e}")

    print("Salvando dados das outras moedas...")
    try:
        df_others.to_sql('other_coins', con=engine, if_exists='replace', index=False)
        print("Dados das outras moedas salvos em 'other_coins'.")
    except Exception as e:
        print(f"Erro ao salvar other_coins: {e}")

def fetch_data_from_sqlite(table_name: str):
    """
    Busca dados de uma tabela específica no banco de dados SQLite e retorna como DataFrame.
    """
    engine = get_sqlite_engine()
    print(f"Buscando dados da tabela '{table_name}'...")
    try:
        df = pd.read_sql_table(table_name, con=engine)
        print(f"Dados da tabela '{table_name}' carregados com sucesso.")
        return df
    except Exception as e:
        print(f"Erro ao buscar dados da tabela '{table_name}': {e}")
        return pd.DataFrame() # Retorna um DataFrame vazio em caso de erro

if __name__ == "__main__":
    # Este bloco é para testes isolados do database_manager.py
    # Em um cenário real, você chamaria essas funções de main.py
    print("--- Testando database_manager.py ---")
    
    # Criar um DataFrame de exemplo para teste
    sample_data_top5 = {
        'rank': [1, 2],
        'symbol': ['BTC', 'ETH'],
        'name': ['Bitcoin', 'Ethereum'],
        'priceUsd': [60000.0, 3000.0],
        'marketCapUsd': [1200000000000.0, 360000000000.0],
        'volumeUsd24Hr': [20000000000.0, 10000000000.0],
        'changePercent24Hr': [2.5, 1.8]
    }
    df_sample_top5 = pd.DataFrame(sample_data_top5)

    sample_data_others = {
        'rank': [6, 7],
        'symbol': ['ADA', 'SOL'],
        'name': ['Cardano', 'Solana'],
        'priceUsd': [0.45, 150.0],
        'marketCapUsd': [15000000000.0, 60000000000.0],
        'volumeUsd24Hr': [500000000.0, 3000000000.0],
        'changePercent24Hr': [-1.2, 5.0]
    }
    df_sample_others = pd.DataFrame(sample_data_others)

    # Cria as tabelas e salva os dados de exemplo
    engine = get_sqlite_engine()
    create_tables(engine) # Chame explicitamente para criar as tabelas com os tipos definidos
    save_dataframes_to_sqlite(df_sample_top5, df_sample_others)

    # Busca os dados e exibe
    df_fetched_top5 = fetch_data_from_sqlite('top_5_coins')
    print("\nDados recuperados de 'top_5_coins':")
    print(df_fetched_top5.to_string(index=False))

    df_fetched_others = fetch_data_from_sqlite('other_coins')
    print("\nDados recuperados de 'other_coins':")
    print(df_fetched_others.to_string(index=False))