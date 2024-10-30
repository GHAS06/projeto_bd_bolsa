import zipfile
import pandas as pd 

# Caminho para o arquivo .zip
caminho_zip = r'C:\Users\FABIA\Downloads\202402_NovoBolsaFamilia.zip'

# Lendo o arquivo .zip e listando todos os nomes de arquivos dentro dele
with zipfile.ZipFile(caminho_zip, 'r') as meu_zip:  
    print(meu_zip.namelist())
    
    # Lendo o arquivo CSV dentro do .zip com pandas
    with meu_zip.open('202402_NovoBolsaFamilia.csv') as arquivo_csv:
        # Tentando ler o CSV com uma codificação diferente
        df = pd.read_csv(arquivo_csv, encoding='ISO-8859-1', nrows= 10000)  # ou 'latin1'
        
    print(df.head())  # Mostrar as 5 primeiros linhas 