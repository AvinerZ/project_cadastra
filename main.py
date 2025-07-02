# main.py
import pandas as pd
from api_client import get_all_assets_v3, get_asset_by_id_v3
from database_manager import (
    save_dataframes_to_sqlite,
    fetch_data_from_sqlite,
    create_tables,
    get_sqlite_engine,
)


def process_and_display_assets(all_assets_data):
    """
    Processa os dados dos ativos em DataFrames do Pandas e os exibe.
    Cria duas tabelas: as 5 primeiras moedas e as demais.
    Também salva essas tabelas no SQLite.
    """
    if not all_assets_data:
        print("Nenhum dado de ativo foi retornado. Impossível criar tabelas.")
        return

    # 1. Criar um DataFrame do Pandas a partir dos dados brutos
    df = pd.DataFrame(all_assets_data)

    # Assegurar que 'rank' e colunas numéricas estão no tipo correto
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["priceUsd"] = pd.to_numeric(df["priceUsd"], errors="coerce")
    df["marketCapUsd"] = pd.to_numeric(df["marketCapUsd"], errors="coerce")
    df["volumeUsd24Hr"] = pd.to_numeric(df["volumeUsd24Hr"], errors="coerce")
    df["changePercent24Hr"] = pd.to_numeric(df["changePercent24Hr"], errors="coerce")

    df.dropna(subset=["rank"], inplace=True)
    df.sort_values(by="rank", inplace=True)

    # Selecionar apenas as colunas de interesse para as tabelas do banco de dados
    # Mantenha os tipos numéricos aqui para salvar no banco corretamente
    columns_for_db = [
        "rank",
        "symbol",
        "name",
        "priceUsd",
        "marketCapUsd",
        "volumeUsd24Hr",
        "changePercent24Hr",
    ]
    df_for_db = df[columns_for_db].copy()
    df_for_db.fillna(0, inplace=True)  # Preencher NaNs com 0 para o banco de dados

    # Criar a tabela com as 5 primeiras moedas (dados numéricos)
    df_top_5_db = df_for_db.head(5)

    # Criar a tabela com as outras moedas (dados numéricos)
    df_other_coins_db = df_for_db.iloc[5:]

    # --- SALVAR NO BANCO DE DADOS SQLite ---
    engine = get_sqlite_engine()  # Obter o engine
    create_tables(engine)  # Criar as tabelas no DB (se não existirem)
    save_dataframes_to_sqlite(df_top_5_db, df_other_coins_db)
    # --- FIM DO SALVAMENTO ---

    # Para exibição, podemos formatar os dados
    df_display = df_for_db.copy()  # Criar uma cópia para formatação de exibição
    df_display["priceUsd"] = df_display["priceUsd"].map("${:,.2f}".format)
    df_display["marketCapUsd"] = df_display["marketCapUsd"].map("${:,.2f}".format)
    df_display["volumeUsd24Hr"] = df_display["volumeUsd24Hr"].map("${:,.2f}".format)
    df_display["changePercent24Hr"] = df_display["changePercent24Hr"].map(
        "{:,.2f}%".format
    )

    # Exibição das tabelas no console (agora usando df_display)
    top_5_coins_display = df_display.head(5)
    print("\n" + "=" * 50)
    print("--- As 5 Primeiras Criptomoedas por Rank (Exibição) ---")
    print(top_5_coins_display.to_string(index=False))
    print("=" * 50 + "\n")

    other_coins_display = df_display.iloc[5:]
    print("\n" + "=" * 50)
    print("--- Outras Criptomoedas (do Rank 6 em diante - Exibição) ---")
    print(other_coins_display.head(10).to_string(index=False))

    if len(other_coins_display) > 10:
        print(f"\n... (e mais {len(other_coins_display) - 10} moedas)")
    print("=" * 50 + "\n")

    # Exemplo de busca de ativo específico (Bitcoin)
    print("\n--- Exemplo de busca de ativo específico (Bitcoin) ---")
    bitcoin_info = get_asset_by_id_v3("bitcoin")
    if bitcoin_info:
        print(f"Nome: {bitcoin_info.get('name')}")
        print(f"Símbolo: {bitcoin_info.get('symbol')}")
        print(f"Preço (USD): ${float(bitcoin_info.get('priceUsd')):.2f}")
    else:
        print("Não foi possível obter informações para Bitcoin.")

    # Exemplo de como recuperar dados do SQLite após salvá-los
    print("\n--- Verificando dados recuperados do SQLite ---")
    retrieved_top_5 = fetch_data_from_sqlite("top_5_coins")
    if not retrieved_top_5.empty:
        print("\nDados de 'top_5_coins' recuperados do DB:")
        print(retrieved_top_5.head(5).to_string(index=False))

    retrieved_others = fetch_data_from_sqlite("other_coins")
    if not retrieved_others.empty:
        print("\nDados de 'other_coins' recuperados do DB (primeiros 5):")
        print(retrieved_others.head(5).to_string(index=False))


if __name__ == "__main__":
    all_assets = get_all_assets_v3()
    process_and_display_assets(all_assets)
