import psycopg2
import random
import time
import datetime
import argparse
from argparse import ArgumentParser

parser = argparse.ArgumentParser(description='Testes CockroachDB')
parser.add_argument('--db_host', type=str, help='hostname do banco de dados', required=True)
parser.add_argument('--db_name', type=str, help='database name', required=True)
parser.add_argument('--db_user', type=str, help='user database', required=True)
parser.add_argument('--db_pass', type=str, help='pass database', required=True)
parser.add_argument('--db_port', type=int, help='port database', required=True)
parser.add_argument('--table_name', type=str, help='name table', required=True)
parser.add_argument('--table_size', type=int, help='quantidade de linhas por tabela', required=True)
parser.add_argument('--table_count', type=int, help='quantidade de tabelas', required=True)
parser.add_argument('--action', type=str, help='defina a acao, ex. create, delete', required=True)

DB_HOST = parser.parse_args().db_host
DB_NAME = parser.parse_args().db_name
DB_USER = parser.parse_args().db_user
DB_PASS = parser.parse_args().db_pass
DB_PORT = parser.parse_args().db_port
TABLE_NAME = parser.parse_args().table_name
TABLE_SIZE = parser.parse_args().table_size
TABLE_COUNT = parser.parse_args().table_count

def create_table():
    for i in range(1, TABLE_COUNT):
        cur.execute('CREATE TABLE IF NOT EXISTS ' + TABLE_NAME + str(i) +'(id SERIAL PRIMARY KEY, date TEXT, '\
                    'prod_name TEXT, valor REAL)')
        print("CREATE TABLE: " + TABLE_NAME + str(i) + " - OK")
        time.sleep(1)
        conn.commit()

## Usando variaveis para inserir dados
def data_insert_var():
    new_date = datetime.datetime.now()
    new_prod_name = 'CockroachDB'
    #new_valor = random.randrange(50,100)
    new_valor = "0"
    for i in range(1, TABLE_COUNT):
        cur.execute("INSERT INTO " + TABLE_NAME + str(i) + "(date, prod_name, valor) VALUES (" +"'"+ str(new_date) +"'"+","+"'"+ new_prod_name +"'"+","+ new_valor +");")
        print("Inserindo dados na tabela " + TABLE_NAME + str(i) + " " + str(new_date) + " - OK" )
        time.sleep(1)
        conn.commit()

def remove_table():
    for i in range(1, TABLE_COUNT):
        cur.execute("DROP TABLE IF EXISTS " + TABLE_NAME + str(i))
        print("DELETANDO " + TABLE_NAME + str(i))
        time.sleep(1)
        conn.commit()

def delete_database():
    cur.execute("DROP DATABASE IF EXISTS " + db_name)
    conn.commit()

## Leitura todos os dados
def leitura_todos_dados():
    cur.execute("SELECT * FROM PRODUTOS")
    for linha in cur.fetchall():
        print(linha)
##update
def atualiza_dados():
    cur.execute("UPDATE produtos SET valor = 10.00 WHERE valor = 80.0")
    conn.commit()

#delete
def remove_dados():
    cur.execute("DELETE FROM produtos WHERE valor = 60.0")
    conn.commit()

#Encerrando a conexao
def end_conexao():
    cur.close()
    conn.close()

conn = psycopg2.connect(dbname=DB_NAME, user=DB_USER, password=DB_PASS, host=DB_HOST, port=DB_PORT)
if conn is not None:
    print('Connection established to CockroachDB')
else:
    print('Connection not established to CockroachDB')

cur = conn.cursor()

if parser.parse_args().action == "create":
    create_table()
    data_insert_var()
elif parser.parse_args().action == "delete":
    remove_table()
else:
    print("Opcao errada, execute novamente com -h")

end_conexao()
