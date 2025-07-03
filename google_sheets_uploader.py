import pandas as pd
import gspread
import os
from dotenv import load_dotenv

load_dotenv()

# Caminho para o arquivo JSON de credenciais da conta de serviço
GOOGLE_SHEETS_CREDENTIALS_PATH = os.getenv("GOOGLE_SHEETS_CREDENTIALS_PATH")

if not GOOGLE_SHEETS_CREDENTIALS_PATH or not os.path.exists(
    GOOGLE_SHEETS_CREDENTIALS_PATH
):
    raise ValueError(
        "A variável de ambiente 'GOOGLE_SHEETS_CREDENTIALS_PATH' não está definida "
        "ou o arquivo de credenciais não foi encontrado no caminho especificado. "
        "Por favor, verifique seu arquivo .env e o caminho do arquivo JSON."
    )




def authenticate_gspread():
    try:
        gc = gspread.service_account(filename=GOOGLE_SHEETS_CREDENTIALS_PATH)
        print("Autenticação com Google Sheets bem-sucedida.")
        return gc
    except Exception as e:
        print(f"Erro ao autenticar com Google Sheets: {e}")
        print(
            "Verifique se o caminho das credenciais está correto e se as APIs (Google Drive API e Google Sheets API) estão ativadas no seu projeto GCP."
        )
        raise





def upload_dataframe_to_sheet(
    gc: gspread.Client, dataframe: pd.DataFrame, sheet_name: str
):
    try:
        spreadsheet = gc.open(sheet_name)
        worksheet = spreadsheet.sheet1

        print(f"Limpando e carregando dados para a planilha '{sheet_name}'...")

        worksheet.clear()

        # Converte o DataFrame para uma lista de listas (incluindo o cabeçalho)
        data_to_upload = [dataframe.columns.tolist()] + dataframe.values.tolist()

        # Atualiza todas as células de uma vez
        worksheet.update(data_to_upload)

        print(f"Dados carregados com sucesso para a planilha '{sheet_name}'.")
    except gspread.exceptions.SpreadsheetNotFound:
        print(
            f"Erro: Planilha '{sheet_name}' não encontrada. "
            "Verifique se o nome está EXATO e se a conta de serviço tem acesso de 'Editor' à planilha."
        )
        raise
    except Exception as e:
        print(f"Erro ao carregar dados para a planilha '{sheet_name}': {e}")
        raise
