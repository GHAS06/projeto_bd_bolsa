import psycopg2 

# faz integração com o banco de dados localmente! 
def conf_conexao():
    connection = psycopg2.connect(
        user="postgres",
        password="32481024",
        host="localhost",
        port="5432",
        database=""
        )