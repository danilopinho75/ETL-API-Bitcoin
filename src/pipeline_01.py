import requests
from tinydb import TinyDB
from datetime import datetime
import time

def extract_data_bitcoin():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    data = response.json()
    return data

def transform_data_bitcoin(data):
    valor = data["data"]["amount"]
    criptomoeda = data["data"]["base"]
    moeda = data["data"]["currency"]
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    data_transform = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda,
        "timestamp": timestamp
    }

    return data_transform

def save_data_tinydb(data, db_name="bitcoin.json"):
    db = TinyDB(db_name)
    db.insert(data)
    print("Dados salvos com sucesso!")


if __name__ == "__main__":
    while True:
        data_json = extract_data_bitcoin()
        data_processed = transform_data_bitcoin(data_json)
        save_data_tinydb(data_processed)
        time.sleep(15)