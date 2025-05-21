import mysql.connector
import pandas as pd
import os 
from dotenv import load_dotenv

'''Coletando os dados tratados que estão em um arquivo em disco, e salvando no MySQL'''

def create_connect_db(host,user,pw):
    '''
    estabelece a conexão com o servidor MySQL,
    utilizando os dados do host, usuário e senha
    '''
    cnx = mysql.connector.connect(
        host = host,
        user = user,
        password = pw
    )
    return cnx

def create_cursor(cnx):
    '''
    cria e retorna um cursor, que serve para conseguirmos
    executar os comandos SQL, utilizando a conexão fornecida como argumento.
    '''
    cursor = cnx.cursor()
    return cursor
    
def create_database(cursor, db_name):
    '''
    cria um banco de dados com o nome fornecido como argumento.
    '''
    try:
        cursor.execute(f'CREATE SCHEMA IF NOT EXISTS {db_name};')
        print(f'Banco de dados {db_name}, criado com sucesso!')
    except Exception as e:
        print(e)

def show_databases(cursor):
    '''
    exibe todos os bancos de dados existentes. 
    '''
    cursor.execute('SHOW DATABASES;')
    for db in cursor:
        print(db)

def create_product_table(cursor, db_name, tb_name):
    '''
    cria uma tabela com o nome fornecido no banco de dados especificado.
    '''
    try:
        cursor.execute(f'''CREATE TABLE IF NOT EXISTS {db_name}.{tb_name} (
            id VARCHAR(100),
            Produto VARCHAR(100),
            Categoria_Produto VARCHAR(100),
            Preco FLOAT(10,2),
            Frete FLOAT(10,2),
            Data_Compra DATE,
            Vendedor VARCHAR(100),
            Local_Compra VARCHAR(100),
            Avaliacao_Compra INT,
            Tipo_Pagamento VARCHAR(100),
            Qntd_Parcelas INT,
            Latitude FLOAT(10,2),
            Longitude FLOAT(10,2),
            
            PRIMARY KEY(id)
            
            );'''
        )
    except Exception as e:
        print(e)
    
def show_tables(cursor, db_name):
    '''
    lista todas as tabelas existentes no banco de dados especificado.
    '''
    cursor.execute(f"SHOW TABLES FROM {db_name};")
    for table in cursor:
        print(table)
    
def read_csv(path):
    '''
    lê um arquivo csv do caminho fornecido e retorna um DataFrame do pandas com esses dados.
    '''
    df = pd.read_csv(path)
    return df
    
def add_product_data(cnx, cursor, df, db_name, tb_name):
    '''
    insere os dados do DataFrame fornecido à tabela especificada no banco de dados especificado.
    '''
    try:
        list_data = [tuple(row) for i, row in df.iterrows() ] 
        query_sql = f'INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'
        
        cursor.executemany(query_sql,list_data)
        cnx.commit()
        print('dados inseridos com sucesso!')
    except Exception as e:
        cnx.rollback()
        print(e)
    
load_dotenv()
host = os.getenv('host')
user = os.getenv('user')
password = os.getenv('password')

db_name = 'db_produtos_teste'
tb_name = 'tb_livros'

cnx = create_connect_db(host,user,password)
cursor = create_cursor(cnx)
create_database(cursor,db_name)
show_databases(cursor)
create_product_table(cursor,db_name,tb_name)
show_tables(cursor,db_name)
df_livros = read_csv('data/tabela_livros.csv')
add_product_data(
    cnx,
    cursor,
    df_livros,
    db_name,
    tb_name
)
cursor.close()
cnx.close()