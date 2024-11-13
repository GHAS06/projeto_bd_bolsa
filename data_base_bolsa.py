import zipfile
import pandas as pd
from sqlalchemy import create_engine
import time
import os
import psutil
import time

# Define a prioridade do processo atual
processo_atual = psutil.Process(os.getpid())
processo_atual.nice(psutil.HIGH_PRIORITY_CLASS)   # Para prioridade alta

# Informa o PID do nosso processo
print(f"Processo em execução com PID: {os.getpid()}")

# Caminho para a pasta Downloads
caminho_downloads = r"C:\Users\FABIA\Downloads"

# Caminho completo do arquivo zip
caminho_zip = os.path.join(caminho_downloads, "202403_NovoBolsaFamilia.zip")

# Etapa 1: Extrair o arquivo CSV de dentro do arquivo .zip
with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
    start_time = time.time()
    arq_csv = zip_ref.namelist()[0]
    print(f"Listando o arquivo.csv: {arq_csv}")    
    
    # Extraindo o arquivo para a pasta Downloads
    zip_ref.extract(arq_csv, caminho_downloads)
    print(f"Arquivo extraído para o caminho: {os.path.join(caminho_downloads, arq_csv)}")
    time_end = time.time()
    real_time = time_end - start_time
    print(f'Tempo de extração: {real_time:.2f} segundos')

# Caminho completo do CSV extraído
caminho_csv_extraido = os.path.join(caminho_downloads, arq_csv)

# Pré-processamento do arquivo
with open(caminho_csv_extraido, 'r', encoding='latin1') as file:
    content = file.read()
    content = content.replace(',', '.')  # Substitui todas as vírgulas por pontos

with open(caminho_csv_extraido, 'w', encoding='latin1') as file:
    start_time = time.time()
    file.write(content)
    time_end = time.time()
    real_time = time_end - start_time
    print(f'Tempo de pré-processamento: {real_time:.2f} segundos')


# Carregar o CSV para um DataFrame sem forçar o tipo de dado inicialmente
df = pd.read_csv(caminho_csv_extraido, delimiter=';', encoding='latin1')

# Renomear as colunas conforme necessário
df.rename(columns={
    'MÊS REFERÊNCIA': 'data_referencia',
    'MÊS COMPETÊNCIA': 'data_competencia',
    'UF': 'uf',
    'CÓDIGO MUNICÍPIO SIAFI': 'codigo_municipio',
    'NOME MUNICÍPIO': 'nome_municipio',
    'CPF FAVORECIDO': 'cpf_beneficiario',
    'NIS FAVORECIDO': 'nis_beneficiario',
    'NOME FAVORECIDO': 'nome_beneficiario',
    'VALOR PARCELA': 'valor_parcela'
}, inplace=True)

# Tratar a coluna 'valor_parcela' corretamente
df['valor_parcela'] = pd.to_numeric(df['valor_parcela'].astype(str).str.replace(',', '.'), errors='coerce')

# Conectar ao banco de dados PostgreSQL
user = 'postgres'
password = '32481024'
host = 'localhost'
database = 'atividade_bolsa'
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}')

# Variável para contar o total de linhas importadas
total_linhas_importadas = 0

# Etapa 3: Inserir os dados no banco de dados em lotes com pausa
try:
    print("Conexão realizada!\nInício da exportação dos dados...")
    start_time = time.time()

    for i, chunk in enumerate(pd.read_csv(caminho_csv_extraido, delimiter=';', encoding='latin1', chunksize=1000000)):
        chunk.columns = df.columns  # Garantir que as colunas estejam corretas
        
        # Garantir que 'valor_parcela' seja numérico (sem necessidade de manipular novamente com .str.replace)
        chunk['valor_parcela'] = pd.to_numeric(chunk['valor_parcela'].astype(str).str.replace(',', '.'), errors='coerce')
        
        # Inserir no banco
        chunk.to_sql('pagamentos', engine, if_exists='append', index=False )
        
        total_linhas_importadas += len(chunk)
        print(f"Lote {i + 1} inserido com sucesso. Total de linhas importadas até agora: {total_linhas_importadas}")
        time.sleep(3)

except Exception as e:
    print(f"Ocorreu um erro ao inserir os dados: {e}")

# Tempo total de importação
time_end = time.time()
real_time = time_end - start_time
print(f'Tempo de importação: {real_time:.2f} segundos')

# Exibir o total de linhas importadas
print(f"Total de linhas importadas: {total_linhas_importadas}")

# Remover o arquivo CSV extraído após a operação
os.remove(caminho_csv_extraido)
print("Arquivo CSV removido com sucesso.")
