import pandas as pd
from api_client import get_all_assets_v3
from database_manager import save_dataframes_to_sqlite, create_tables, get_sqlite_engine
from google_sheets_uploader import authenticate_gspread, upload_dataframe_to_sheet

# Nomes EXATOS das planilhas no Google Sheets
TOP_5_SHEET_NAME = "top criptos"
OTHER_COINS_SHEET_NAME = "outras criptos"


def process_and_persist_crypto_data():
    """
    Orquestra a busca de dados da API, o processamento,
    o salvamento no SQLite e o upload para o Google Sheets.
    """
    print("--- Iniciando a Pipeline de Dados de Criptomoedas ---")

    all_assets_data = get_all_assets_v3()

    if not all_assets_data:
        print("Nenhum dado de ativo foi retornado da API. A pipeline será finalizada.")
        return

    # 1. Processar dados com Pandas
    print("\nProcessando dados com Pandas...")
    df = pd.DataFrame(all_assets_data)

    # Conversão de tipos numéricos e tratamento de NaNs
    df["rank"] = pd.to_numeric(df["rank"], errors="coerce")
    df["priceUsd"] = pd.to_numeric(df["priceUsd"], errors="coerce")
    df["marketCapUsd"] = pd.to_numeric(df["marketCapUsd"], errors="coerce")
    df["volumeUsd24Hr"] = pd.to_numeric(df["volumeUsd24Hr"], errors="coerce")
    df["changePercent24Hr"] = pd.to_numeric(df["changePercent24Hr"], errors="coerce")

    df.dropna(subset=["rank"], inplace=True)  # Remove linhas sem rank
    df.sort_values(by="rank", inplace=True)  # Ordena por rank

    # Seleção de colunas para salvamento/upload (mantém tipos numéricos)
    columns_for_output = [
        "rank",
        "symbol",
        "name",
        "priceUsd",
        "marketCapUsd",
        "volumeUsd24Hr",
        "changePercent24Hr",
    ]
    df_processed = df[columns_for_output].copy()
    df_processed.fillna(0, inplace=True)  # Preenche NaNs com 0

    # Divide os DataFrames
    df_top_5 = df_processed.head(5)
    df_other_coins = df_processed.iloc[5:]
    print("Dados processados e divididos em DataFrames.")

    # 2. Salvar no Banco de Dados SQLite
    print("\n--- Iniciando Salvamento no SQLite ---")
    try:
        engine = get_sqlite_engine()
        create_tables(engine)  # Garante que as tabelas existam
        save_dataframes_to_sqlite(df_top_5, df_other_coins)
        print("Dados salvos no SQLite com sucesso.")
    except Exception as e:
        print(f"Erro durante o salvamento no SQLite: {e}")
    print("--- Fim do Salvamento no SQLite ---")

    # 3. Upload para Google Sheets
    print("\n--- Iniciando Upload para Google Sheets ---")
    try:
        gc = authenticate_gspread()  # Autentica com o Google Sheets
        upload_dataframe_to_sheet(gc, df_top_5, TOP_5_SHEET_NAME)
        upload_dataframe_to_sheet(gc, df_other_coins, OTHER_COINS_SHEET_NAME)
        print("Upload para Google Sheets concluído com sucesso!")
    except Exception as e:
        print(f"Falha ao fazer upload para Google Sheets: {e}")
    print("--- Fim do Upload para Google Sheets ---")

    print("\n--- Pipeline de Dados Concluída ---")


if __name__ == "__main__":
    process_and_persist_crypto_data()
