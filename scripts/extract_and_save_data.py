from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from datetime import datetime
import requests
import os 
from dotenv import load_dotenv

'''Extraindo dados de uma api, e salvando o conteudo no MongoDb'''

def connect_mongo(uri:str) -> MongoClient:
    '''
    estabelece a conexão com a instância do MongoDB usando a URI fornecida.
    Ela retorna o cliente do MongoDB que permite interagir com o banco de dados.
    '''
    client = MongoClient(uri,server_api=ServerApi('1'))#server api é a versao do servidor que estamos utlizando para fazer a conexao
    try:
        client.admin.command('ping')
        print('Conexão realizada com o servidor MongoDb relizada com sucesso!')
        return client
    except Exception as e:
        raise ValueError(e)
        
def create_connect_db(client:MongoClient,db_name:str):
    '''
    utiliza o(a) cliente do MongoDB para criar (se não existir) e conectar-se ao banco de dados
    especificado pelo parâmetro db_name. Ela retorna o objeto de banco de dados que pode ser
    usado para interagir com as coleções dentro dele.
    '''
    try:
        db = client[db_name]
        print('conexao com o db estabelecida')
        return db
    except Exception as e:
        raise ValueError(e)

def create_connect_collection(db, col_name:str):
    '''cria (se não existir) e conecta-se à coleção especificada pelo parâmetro col_name dentro
    do banco de dados fornecido. Ela retorna o objeto de coleção que pode ser usado para 
    interagir com os documentos dentro dela.
    '''
    try:
        collection = db[col_name]
        print('conexao com a collection estabelecida')
        return collection
    except Exception as e:
        raise ValueError(e)

def extract_api_data(url:str) -> dict:
    '''extrai dados de uma API na URL fornecida e retorna os dados extraídos no formato JSON.'''
    url = 'https://labdados.com/produtos'
    response = requests.get(url)
    if response.status_code != 200:
        raise ValueError("conteudo não encontrado")
    dados_json = response.json()
    print('Dados da api resgatados com sucesso')
    return dados_json
        
def insert_data(col,data:dict):
    ''' recebe uma coleção e os dados que serão inseridos nela.
    Ela deve adicionar todos os documentos à coleção e
    retornar a quantidade de documentos inseridos.
    '''
    try:
        docs = col.insert_many(data)
        qtd_docs_inserted = len(docs.inserted_ids)
        print('qtd de documentos inseridos '+str(qtd_docs_inserted))
        return qtd_docs_inserted
    except Exception as e:
        raise ValueError(e)
    
def close_connection_db(client:MongoClient):
    '''Encerra a conexão com o MongoDb'''
    try:
        client.close()
        print('Conexão encerrada com sucesso')
    except Exception as e:
        raise ValueError(e)
    
load_dotenv()
'''
inicio = datetime.now()

uri = 'os.getenv('uri_mongo')
client = connect_mongo(uri)
db = create_connect_db(client,'db_desafio')
collection = create_connect_collection(db,'desafio')
url = 'https://labdados.com/produtos'
data_api_json = extract_api_data(url)
qtd_docs_insert = insert_data(collection,data_api_json)
close_connection_db(client)

tempo_total = datetime.now() - inicio
print(f'tempo total de execução {tempo_total} ')
'''