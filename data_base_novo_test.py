import zipfile
import pandas as pd
from sqlalchemy import create_engine
import time  # Importa o módulo para adicionar pausas

# Caminhos dos arquivos
zip_file_path = 'C:/Users/FABIA/Downloads/202403_NovoBolsaFamilia.zip'
csv_file_name = '202403_NovoBolsaFamilia.csv'
extracted_folder = 'C:/Users/FABIA/Downloads/'

# Etapa 1: Extrair o arquivo CSV de dentro do arquivo .zip
with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extract(csv_file_name, extracted_folder)

# Caminho completo para o arquivo CSV extraído
csv_file_path = f'{extracted_folder}/{csv_file_name}'

# Etapa 2: Carregar o arquivo CSV usando o Pandas
df = pd.read_csv(csv_file_path)

# Renomear as colunas do DataFrame para que correspondam aos nomes no banco de dados
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

# Conversão de tipos e limpeza de dados
df['valor_parcela'] = df['valor_parcela'].str.replace(',', '.').astype(float)  # Corrigir valores decimais
df['cpf_beneficiario'] = df['cpf_beneficiario'].str.replace('', '').fillna('')  # Remover '' e NaN

# Visualizar as primeiras linhas para verificar se a transformação foi bem-sucedida
print("Dados carregados e transformados:")
print(df.head())

# Etapa 3: Conectar ao banco de dados PostgreSQL usando SQLAlchemy
user = 'postgres'       # substitua pelo seu usuário
password = '32481024'     # substitua pela sua senha
host = 'localhost'         # ou o host apropriado
database = 'atividade_banco'

engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{database}')

# Etapa 4: Inserir os dados no banco de dados em lotes com pausa
try:
    print("Conexão realizado!")
    # Insere os dados em lotes de 1.000.000 registros
    for i, chunk in enumerate(pd.read_csv(csv_file_path, chunksize=1000000, encoding='Latin-1')):
        chunk.to_sql('pagamentos', engine, if_exists='append', index=False)
        print(f"Lote {i + 1} inserido com sucesso.")
        
        # Pausa de 3 segundos entre os lotes
        time.sleep(3)
except Exception as e:
    print(f"Ocorreu um erro ao inserir os dados: {e}")