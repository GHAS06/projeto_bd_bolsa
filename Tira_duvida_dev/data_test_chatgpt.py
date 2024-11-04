import zipfile
import os
import psycopg
import time
import re

# Caminho para o arquivo .zip
caminho_zip = r"C:\Users\FABIA\Downloads\202403_NovoBolsaFamilia.zip"

# Parâmetros de conexão
db_params = {
    "dbname": "Atividade_Bolsa",
    "user": "postgres",
    "password": "32481024",
    "host": "localhost",
    "port": "5432"
}

try:
    # Extraindo o CSV do ZIP
    with zipfile.ZipFile(caminho_zip, 'r') as meu_zip:
        csv_file_name = '202403_NovoBolsaFamilia.csv'  # Nome do arquivo CSV
        meu_zip.extract(csv_file_name)  # Extraindo o arquivo CSV

    # Verificando se o arquivo CSV foi extraído
    if not os.path.exists(csv_file_name):
        raise FileNotFoundError(f"O arquivo {csv_file_name} não foi encontrado após a extração.")

    # Verificando o conteúdo do CSV
    print("Conteúdo do arquivo CSV (primeiras 5 linhas):")
    with open(csv_file_name, 'r', encoding='ISO-8859-1') as arquivo_csv:
        for _ in range(5):
            print(arquivo_csv.readline().strip())

    # Conexão ao banco de dados
    with psycopg.connect(**db_params) as conn:
        with conn.cursor() as cur:
            # Medindo o tempo de inserção
            start_time = time.time()

            try:
                # Usando o COPY para inserir os dados diretamente do CSV
                with open(csv_file_name, 'r', encoding='ISO-8859-1') as arquivo_csv:
                    # Ignorando a primeira linha (cabeçalho)
                    next(arquivo_csv)
                    for linha in arquivo_csv:
                        # Processando a linha
                        dados = linha.strip().split(';')
                        # Remover aspas duplas dos dados
                        dados = [dado.replace('"', '').strip() for dado in dados]

                        # Verificação e limpeza dos dados
                        try:
                            ano_mes_competencia = int(dados[0]) if dados[0] else None
                            ano_mes_referencia = int(dados[1]) if dados[1] else None
                            uf = dados[2] if dados[2] else None
                            codigo_municipio_siafi = int(dados[3]) if dados[3] else None
                            nome_municipio = dados[4] if dados[4] else None
                            
                            # Limpar CPF e NIS
                            cpf_beneficiario = re.sub(r'\D', '', dados[5]) if dados[5] else None
                            nis_beneficiario = re.sub(r'\D', '', dados[6]) if dados[6] else None
                            
                            # Limpar e converter o valor
                            valor_parcela = float(dados[8].replace(',', '.')) if dados[8] else None

                            # Inserindo dados na tabela
                            cur.execute("""
                                INSERT INTO bolsa_familia_pagamentos 
                                (ano_mes_referencia, ano_mes_competencia, uf, codigo_municipio_siafi, 
                                nome_municipio, cpf_beneficiario, nis_beneficiario, nome_beneficiario, valor_parcela) 
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                            """, (ano_mes_referencia, ano_mes_competencia, uf, codigo_municipio_siafi,
                                  nome_municipio, cpf_beneficiario, nis_beneficiario,
                                  dados[7] if len(dados) > 7 else None, valor_parcela))

                        except Exception as inner_e:
                            print(f"Erro ao processar linha: {linha.strip()}")
                            print(inner_e)

                print("Dados inseridos com sucesso!")

                # Confirmar a transação
                conn.commit()  # Garantir que a transação seja confirmada

            except Exception as e:
                print("Erro ao executar a inserção:")
                print(e)
                conn.rollback()  # Desfazer a transação em caso de erro

            # Calculando o tempo total de inserção
            end_time = time.time()
            tempo_insercao = end_time - start_time
            print(f"Tempo total de inserção: {tempo_insercao:.2f} segundos")

            # Verificando o número total de linhas inseridas
            cur.execute("SELECT COUNT(*) FROM bolsa_familia_pagamentos;")
            total_rows = cur.fetchone()[0]
            print(f"Número total de linhas na tabela: {total_rows}")

            # Exibindo as 10 primeiras linhas inseridas
            cur.execute("SELECT * FROM bolsa_familia_pagamentos LIMIT 10;")
            linhas = cur.fetchall()
            print("Exibindo as 10 primeiras linhas inseridas:")
            for linha in linhas:
                print(linha)

    # Limpeza do arquivo CSV
    os.remove(csv_file_name)
    print("Arquivo CSV removido com sucesso.")

except FileNotFoundError as e:
    print(e)
except psycopg.Error as e:
    print("Erro ao conectar ao banco de dados ou durante a execução do SQL.")
    print(e)
except Exception as e:
    print("Um erro inesperado ocorreu:")
    print(e)
finally:
    print("Processo concluído.")
