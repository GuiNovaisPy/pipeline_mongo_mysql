import pandas as pd
import os 
from dotenv import load_dotenv
from extract_and_save_data import (
    connect_mongo,
    create_connect_db,
    create_connect_collection,
    close_connection_db
)


'''Transformando conteudo alocado no MongoDb, e salvando em arquivos csv'''


def visualize_collection(col):
    '''imprime todos os documentos existentes na coleção.'''
    for doc in col.find({}):
        print(doc)
    
    
def rename_column(col,col_name_old,new_name):
    '''renomeia uma coluna existente.'''
    try:
        col.update_many({},{'$rename':{col_name_old:new_name}})
        print(f"Coluna {col_name_old} renomeada para {new_name}.")
    except Exception as e:
        print(e)
        print(f"Erro ao renomear a coluna: {e}")
    

def select_category(col,category):
    '''seleciona documentos que correspondam a uma categoria específica.'''
    query = {'Categoria do Produto': category}
    list_category = list(col.find(query))
    return list_category
    
    
def make_regex(col,filter_parameter, regex):
    '''seleciona documentos que correspondam a uma expressão regular específica.'''
    query = {filter_parameter : {'$regex': regex}}
    list_data = list(col.find(query))
    return list_data

def create_dataframe(lista_data):
    '''cria um dataframe a partir de uma lista de documentos.'''
    try:
        df = pd.DataFrame(lista_data)
        print('dataframe criado')
        return df
    except Exception as e:
        print(e)

def format_date(df:pd.DataFrame,columns):
    '''formata a coluna de datas do dataframe para o formato "ano-mes-dia".'''
    try:
        for column in columns:
            df[column] = pd.to_datetime(df[column],format='%d/%m/%Y')
            df[column] = df[column].dt.strftime('%Y-%m-%d')
        print('Datas convertidas com sucesso')
        return df
    except Exception as e:
        print(e)
        
def save_csv(df:pd.DataFrame, path):
    '''salva o dataframe como um arquivo CSV no caminho especificado.'''
    try:
        df.to_csv(path,index= False)
        print(f'csv salvo em : {path}')
    except Exception as e:
        print(e)
        
load_dotenv()

uri = os.getenv('uri_mongo')
client = connect_mongo(uri)
db = create_connect_db(client,'db_desafio')
collection = create_connect_collection(db,'desafio')
visualize_collection(collection)

'''renomeando a colunas'''
rename_column(collection,'lat','Latitude')
rename_column(collection,'lon','Longitude')

'''tabela livros'''
dados_livros = select_category(collection,'livros')
df_dados_livros = create_dataframe(dados_livros)
df_dados_livros = format_date(df_dados_livros,['Data da Compra'])
save_csv(df_dados_livros,'data/tabela_livros.csv')

'''tabela venda 2021 em diante'''
dados_2021 = make_regex(collection,'Data da Compra','/202[1-9]')
df_dados_2021 = create_dataframe(dados_2021)
df_dados_2021 = format_date(df_dados_2021,['Data da Compra'])
save_csv(df_dados_2021,'data/tabela_2021_em_diante.csv')

close_connection_db(client)
