import requests
from datetime import datetime
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
import os

from database import Base, BitcoinPrice

# Carrega as variáveis de ambiente do arquivo .env
load_dotenv()

# Lê as variáveis separadas do arquivo .env (sem SSL)
POSTGRES_USER = os.getenv('POSTGRES_USER')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD')
POSTGRES_HOST = os.getenv('POSTGRES_HOST')
POSTGRES_PORT = os.getenv('POSTGRES_PORT')
POSTGRES_DB = os.getenv('POSTGRES_DB')

DATABASE_URL = (
    f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Cria a engine e a sessão
engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

def create_table():
    """ Cria a tabela no banco de dados, se não existir."""
    Base.metadata.create_all(engine)
    print("Tabela criada/verificada com sucesso!")

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
def save_data_postgres(data):
    """Salva os dados no banco PostgreSQL"""
    session = Session()
    new_register = BitcoinPrice(**data)
    session.add(new_register)
    session.commit()
    session.close()
    print(f"[{data['timestamp']}] Dados salvos no PostgreSQL")


if __name__ == "__main__":
    create_table()
    print("Iniciando ETL com atualização a cada 2 horas... (CTRL+C para interromper)")

    while True:
        try:
            data_json = extract_data_bitcoin()
            if data_json:
                data_processed = transform_data_bitcoin(data_json)
                print("Dados tratados: ", data_processed)
                save_data_postgres(data_processed)
            time.sleep(7200)
        except KeyboardInterrupt:
            print("\nProcesso interrompido pelo Usuário. Finalizando...")
            break
        except Exception as e:
            print(f"Erro durante a execução: {e}")
            time.sleep(15)