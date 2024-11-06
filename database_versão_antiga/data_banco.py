import psycopg
import zipfile
import os
import csv
import time  # Biblioteca para medir o tempo de execução

# Configurações de conexão com o PostgreSQL
conn_params = {
    "host": "localhost",
    "port": "5432",
    "dbname": "atividade_bolsa",
    "user": "postgres",
    "password": "32481024"
}

# SQL para criar a tabela pagamentos, caso ela não exista
create_table_query = """
CREATE TABLE IF NOT EXISTS pagamentos (
    data_referencia VARCHAR(45),
    data_competencia VARCHAR(45),
    uf VARCHAR(5),
    codigo_municipio VARCHAR(45),
    nome_municipio VARCHAR(45),
    cpf_beneficiario VARCHAR(45),
    nis_beneficiario VARCHAR(45),
    nome_beneficiario VARCHAR(45),
    valor_parcela DECIMAL(10, 2)
);
"""

def criar_tabela_pagamentos():
    """Cria a tabela 'pagamentos' se ela não existir."""
    try:
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                cur.execute(create_table_query)
        print("Tabela 'pagamentos' criada ou já existente.")
    except Exception as e:
        print(f"Erro ao criar tabela: {e}")

def extrair_csv_do_zip(caminho_zip, caminho_arquivo_csv):
    """Extrai o arquivo CSV do arquivo ZIP."""
    try:
        with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
            for nome_arquivo in zip_ref.namelist():
                if nome_arquivo.endswith(".csv"):
                    zip_ref.extract(nome_arquivo, os.path.dirname(caminho_arquivo_csv))
                    os.rename(os.path.join(os.path.dirname(caminho_arquivo_csv), nome_arquivo), caminho_arquivo_csv)
                    return caminho_arquivo_csv
        print("Arquivo CSV não encontrado no ZIP.")
        return None
    except Exception as e:
        print(f"Erro ao extrair o arquivo CSV: {e}")
        return None

def tratar_dados_csv(caminho_arquivo_csv):
    """Prepara os dados do CSV para o PostgreSQL, corrigindo problemas de formatação."""
    linhas_tratadas = []
    try:
        with open(caminho_arquivo_csv, 'r', encoding='latin-1') as f:
            reader = csv.reader(f, delimiter=';')
            # Ignora a primeira linha (cabeçalho)
            next(reader)
            for linha in reader:
                data_referencia = linha[1].strip()
                data_competencia = linha[0].strip()
                uf = linha[2].strip()
                codigo_municipio = linha[3].strip()
                nome_municipio = linha[4].strip()
                cpf_beneficiario = linha[5].replace('*', '').strip()
                nis_beneficiario = linha[6].strip()
                nome_beneficiario = linha[7].strip()
                valor_parcela = linha[8].replace(",", ".").strip()
                linhas_tratadas.append([
                    data_referencia, data_competencia, uf, codigo_municipio, nome_municipio,
                    cpf_beneficiario, nis_beneficiario, nome_beneficiario, valor_parcela
                ])
        return linhas_tratadas
    except Exception as e:
        print(f"Erro ao tratar dados do CSV: {e}")
        return []

def salvar_dados_tratados(caminho_arquivo_csv, linhas_tratadas):
    """Salva os dados tratados no CSV para importação."""
    try:
        with open(caminho_arquivo_csv, 'w', encoding='latin-1', newline='') as f:
            writer = csv.writer(f, delimiter=';', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            writer.writerow([
                'data_referencia', 'data_competencia', 'uf', 'codigo_municipio', 
                'nome_municipio', 'cpf_beneficiario', 'nis_beneficiario', 
                'nome_beneficiario', 'valor_parcela'
            ])
            writer.writerows(linhas_tratadas)
        print("Dados tratados salvos com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar os dados tratados: {e}")

def importar_csv_para_postgresql(caminho_arquivo_csv, tabela_destino):
    """Importa os dados do CSV para a tabela no PostgreSQL usando o comando COPY e mede o tempo de execução."""
    try:
        with psycopg.connect(**conn_params) as conn:
            with conn.cursor() as cur:
                # Inicia a contagem de tempo
                start_time = time.time()
                with open(caminho_arquivo_csv, 'r', encoding='latin-1') as f:
                    cur.copy(f"""
                        COPY {tabela_destino} (
                            data_referencia, data_competencia, uf, codigo_municipio, nome_municipio,
                            cpf_beneficiario, nis_beneficiario, nome_beneficiario, valor_parcela
                        ) FROM STDIN WITH (
                            FORMAT csv,
                            DELIMITER ';',
                            QUOTE '"',
                            HEADER true
                        )
                    """, f)
                # Finaliza a contagem de tempo
                end_time = time.time()
                tempo_execucao = end_time - start_time
                print(f"Dados importados com sucesso! Tempo de execução: {tempo_execucao:.2f} segundos.")
    except Exception as e:
        print(f"Erro ao importar os dados para o PostgreSQL: {e}")

# Caminho do arquivo zip e tabela de destino
caminho_zip = r"C:\Users\FABIA\Downloads\202403_NovoBolsaFamilia.zip"
caminho_arquivo_csv = r"C:\Users\FABIA\Downloads\202403_NovoBolsaFamilia.csv"
tabela_destino = "pagamentos"

# Executa o fluxo de importação de dados
criar_tabela_pagamentos()
caminho_csv = extrair_csv_do_zip(caminho_zip, caminho_arquivo_csv)

if caminho_csv:
    linhas_tratadas = tratar_dados_csv(caminho_csv)
    salvar_dados_tratados(caminho_csv, linhas_tratadas)
    importar_csv_para_postgresql(caminho_csv, tabela_destino)
else:
    print("Arquivo CSV não encontrado no zip.")
