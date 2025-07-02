import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

COINCAP_API_KEY = os.getenv("COINCAP_API_KEY")
BASE_URL = "https://rest.coincap.io/v3"

if not COINCAP_API_KEY:
    raise ValueError(
        "A chave de API COINCAP_API_KEY não foi encontrada no arquivo .env ou nas variáveis de ambiente."
    )


# Busca uma lista de todos os ativos (criptomoedas) na CoinCap API v3, com um limite máximo de 2000 ativos por requisição.
def get_all_assets_v3(limit=2000):
    url = f"{BASE_URL}/assets"
    params = {"apiKey": COINCAP_API_KEY, "limit": limit}

    try:
        print(f"Buscando até {limit} ativos da CoinCap API v3...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("data")
    except requests.exceptions.HTTPError as http_err:
        print(f"Erro HTTP ao buscar dados: {http_err}")
        if response.status_code == 401:
            print("Verifique sua chave de API. Erro de autenticação (código 401).")
        elif response.status_code == 429:
            print("Limite de taxa excedido. Tente novamente mais tarde (código 429).")
    except requests.exceptions.RequestException as req_err:
        print(f"Erro de conexão ou requisição: {req_err}")
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON. Resposta: {response.text}")
    return None


"""
    Busca dados de uma criptomoeda específica pelo seu ID na CoinCap API v3.
    Retorna um dicionário com os dados da criptomoeda.
    """


def get_asset_by_id_v3(asset_id: str):
    url = f"{BASE_URL}/assets/{asset_id}"
    params = {"apiKey": COINCAP_API_KEY}

    try:
        print(f"Buscando dados para o ativo: {asset_id}...")
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        return data.get("data")
    except requests.exceptions.HTTPError as http_err:
        print(f"Erro HTTP ao buscar dados para {asset_id}: {http_err}")
        if response.status_code == 401:
            print("Verifique sua chave de API. Erro de autenticação (código 401).")
        elif response.status_code == 429:
            print("Limite de taxa excedido. Tente novamente mais tarde (código 429).")
    except requests.exceptions.RequestException as req_err:
        print(f"Erro de conexão ou requisição para {asset_id}: {req_err}")
    except json.JSONDecodeError:
        print(f"Erro ao decodificar JSON para {asset_id}. Resposta: {response.text}")
    return None
