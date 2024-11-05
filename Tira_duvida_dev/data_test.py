import zipfile
import os
import psycopg
import time

db_params = {
    "dbname": "Atividade_Bolsa",
    "user": "postgres",
    "password": "32481024",
    "host": "localhost",
    "port": "5432"
}

# Fazendo conexão com o banco de dados
with psycopg.connect(**db_params) as conn:
    conn.autocommit = True
    # Caminho para o arquivo .zip
    caminho_zip = r"C:\Users\FABIA\Downloads\202403_NovoBolsaFamilia.zip"

    # Lendo o arquivo .zip e listando todos os nomes de arquivos dentro dele
    with zipfile.ZipFile(caminho_zip, 'r') as meu_zip:  
        arquivo_csv = meu_zip.namelist()[0]  # Puxando o primeiro arquivo dentro do zip
        print(f"Nome do arquivo é: {arquivo_csv}")

        # Abrindo o arquivo dentro do zip e extraindo ele
        with meu_zip.open(arquivo_csv):
            print(f'Extraindo o arquivo: {arquivo_csv} de dentro do zip')
            start_time = time.time()
            meu_zip.extract(arquivo_csv)
            end_time = time.time()
            real_time = end_time - start_time
            print(f'Tempo de extração de arquivo: {real_time:.2f} segundos')

        # Pré-processando o arquivo CSV
        temp_file = "temp_file.csv"
        with open(arquivo_csv, 'r', encoding='Latin-1') as f, open(temp_file, 'w', encoding='Latin-1') as temp_f:
            for line in f:
                # Remove aspas e substitui vírgula por ponto no valor_parcela
                modified_line = line.replace('"', '').replace(',', '.')
                temp_f.write(modified_line)

        # Usando o comando COPY para importar os dados
        try:
            with conn.cursor() as cur:
                with open(temp_file, 'r', encoding='Latin-1') as f:
                    next(f)  # Ignora o cabeçalho
                    print("Importando os dados do csv...")

                    start_time = time.time()
                    # Comando COPY adaptado para a tabela
                    cur.copy("""
                        COPY bolsa_familia_pagamentos (
                            ano_mes_referencia,
                            ano_mes_competencia,
                            uf,
                            codigo_municipio_siafi,
                            nome_municipio,
                            cpf_beneficiario,
                            nis_beneficiario,
                            nome_beneficiario,
                            valor_parcela
                        ) FROM STDIN WITH (FORMAT CSV, DELIMITER ';')
                    """, f)
                    end_time = time.time()

                    real_time = end_time - start_time
                    print(f'Tempo total para finalizar a inserção: {real_time:.2f} segundos')

        except Exception as e:
            print(f"Ocorreu um erro durante a importação: {e}")

# Removendo o arquivo CSV temporário
os.remove(temp_file)
print("Importação de dados realizada com sucesso!")
