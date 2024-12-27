import requests

def extract_data_bitcoin():
    url = "https://api.coinbase.com/v2/prices/spot"
    response = requests.get(url)
    data = response.json()
    return data

def transform_data_bitcoin(data):
    valor = data["data"]["amount"]
    criptomoeda = data["data"]["base"]
    moeda = data["data"]["currency"]

    data_transform = {
        "valor": valor,
        "criptomoeda": criptomoeda,
        "moeda": moeda
    }

    return data_transform


if __name__ == "__main__":
    # Extração dos dados
    data_json = extract_data_bitcoin()
    data_processed = transform_data_bitcoin(data_json)
    print(data_processed)