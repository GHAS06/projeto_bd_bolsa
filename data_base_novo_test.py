import zipfile
import pandas as pd
from sqlalchemy import create_engine
import time
import os

# Caminho para a pasta Downloads
caminho_downloads = r"C:\Users\supguilherme\Downloads"

# Caminho completo do arquivo zip
caminho_zip = os.path.join(caminho_downloads, "202403_NovoBolsaFamilia.zip")

# Etapa 1: Extrair o arquivo CSV de dentro do arquivo .zip
with zipfile.ZipFile(caminho_zip, 'r') as zip_ref:
    # Listando o primeiro arquivo que está no zip
    arq_csv = zip_ref.namelist()[0]
    print(f"Listando o arquivo.csv: {arq_csv}")    
    
    # Extraindo o arquivo para a pasta Downloads
    zip_ref.extract(arq_csv, caminho_downloads)
    print(f"Arquivo extraído para o caminho: {os.path.join(caminho_downloads, arq_csv)}")

# Caminho completo do CSV extraído
caminho_csv_extraido = os.path.join(caminho_downloads, arq_csv)

# Etapa 2: Carregar o arquivo CSV usando o Pandas com delimitador correto
# Carregar somente o cabeçalho para definir o DataFrame corretamente
df = pd.read_csv(caminho_csv_extraido, delimiter=';', encoding='latin1', nrows=0)

# Limpar os nomes das colunas (remover aspas e espaços extras)
df.columns = df.columns.str.replace('"', '', regex=True).str.strip().str.replace(';', '', regex=True)

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

# Conectar ao banco de dados PostgreSQL
user = 'postgres'
password = '32481024'
host = 'localhost'
database = 'atividade_bolsa'
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}')

# Etapa 3: Inserir os dados no banco de dados em lotes com pausa, ignorando o cabeçalho após o primeiro lote
try:
    print("Conexão realizada!\n")
    print("Inicio da Exportação dos dados...")
    # Iterar sobre o arquivo em chunks
    for i, chunk in enumerate(pd.read_csv(caminho_csv_extraido, delimiter=';', encoding='latin1', chunksize=1000000)):
        # Se for o primeiro chunk, insira com o cabeçalho; caso contrário, ignore-o
        if i > 0:
            chunk.columns = df.columns  # Atribuir os nomes das colunas do df principal

        # Verificar as colunas do chunk antes de manipular 'valor_parcela'
        print(f"Colunas no chunk {i+1}: {chunk.columns}")
        
        # Verifique a existência da coluna 'valor_parcela' e aplique a conversão
        if 'valor_parcela' in chunk.columns:
            chunk['valor_parcela'] = chunk['valor_parcela'].str.replace(',', '.').astype(float)
        else:
            print(f"Coluna 'valor_parcela' não encontrada no chunk {i+1}. Pulando para o próximo chunk.")
            continue  # Pula para o próximo chunk

        # Inserir no banco
        chunk.to_sql('pagamentos', engine, if_exists='append', index=False)
        print(f"Lote {i + 1} inserido com sucesso.")
        
        # Pausa de 3 segundos entre os lotes
        time.sleep(3)
except Exception as e:
    print(f"Ocorreu um erro ao inserir os dados: {e}")

# Remover o arquivo CSV extraído após a operação
os.remove(caminho_csv_extraido)
print("Arquivo CSV removido com sucesso.")
