import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

# FIXME: Instalar bibliotecas necessárias

# Imports necessários para baixar e extrair os arquivos zip
import tempfile
import zipfile
import os

# Pra começar, vamos importar a url base da API
URL_BASE = "https://dadosabertos.ans.gov.br/FTP/PDA/"

# Recuperar o diretório de demonstrações contábeis, assumindo que ele tenha o nome "demonstracoes_contabeis"
DIR_DEMONS_CONT = f"{URL_BASE}/demonstracoes_contabeis"

# Se certifique de que a resposta do servidor foi OK
resp = requests.get(DIR_DEMONS_CONT)
cod_status = resp.status_code

# Debug
print(f"Status Code: {cod_status}")
# print(f"Response Text: {resp.text[:500]}")  # Print first 500 characters


################### Área de funções auxiliares ###################
##################################################################

# Criar uma função de soup para extrair os anos disponíveis
def extrair_anos_disponiveis(html_text):
    # Parse o HTML usando BeautifulSoup
    soup = BeautifulSoup(html_text, 'html.parser')

    # Encontrar todos os links que são diretórios de anos (formato: 2XXX/)
    links = soup.find_all('a', href=True)
    anos = []
    
    for link in links:
        href = link['href']

        # Extrair anos que seguem o padrão 2XXX/
        match = re.match(r'^(20\d{2})/$', href)
        if match:
            anos.append(int(match.group(1)))
    
    return anos

# Criar uma função para processar os arquivos de acordo com seu formato (.csv, .txt, .xlsx), dado um caminho
# Essa função vai ser usada em baixar_e_extrair_zip()
def processar_arquivo(caminho_arquivo):
    # TODO Correlacionar os dados do arquivo processado (reg ans) com o CNPJ e razão social da empresa
    # Obter arquivo csv de operadoras de plano de saúde ativas
    operadoras_url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
    resp_operadoras = requests.get(operadoras_url)
    if resp_operadoras.status_code == 200:
        # Salvar o conteúdo em um arquivo temporário
        with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
            tmpfile.write(resp_operadoras.content)
            caminho_operadoras = tmpfile.name
            # TODO Realizar o processamento de correlação necessário
            # Retornar uma tupla que contém o registro ans, CNPJ e razão social da empresa
            # Ler o arquivo na variável caminho_arquivo, processando de acordo com o formato
            # Em cada caso, extrair o reg ans e encontrar a correlação com o CNPJ e razão social
            # Salvar ou retornar os dados no formato de tupla (reg_ans, cnpj, razao_social)
            if caminho_arquivo.endswith('.csv'):
                df = pd.read_csv(caminho_arquivo, encoding='latin1', sep=';')

                # ...
            elif caminho_arquivo.endswith('.txt'):
                with open(caminho_arquivo, 'r', encoding='latin1') as f:
                    for line in f:
                        print(line.strip())
            
            elif caminho_arquivo.endswith('.xlsx'):
                
                df = pd.read_excel(caminho_arquivo)

                # ...



        

        print(f"Arquivo de operadoras salvo em: {caminho_operadoras}")
    else:
        print(f"Falha ao baixar o arquivo de operadoras. Status Code: {resp_operadoras.status_code}")
    

# Criar uma função para baixar e extrair arquivos zip
def baixar_e_extrair_zip(url_zip):
    with tempfile.TemporaryDirectory() as tmpdir:
        caminho_do_zip = os.path.join(tmpdir, "arquivo.zip")

        with requests.get(url_zip, stream=True) as r:
            r.raise_for_status()
            with open(caminho_do_zip, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)

        with zipfile.ZipFile(caminho_do_zip, "r") as z:
            z.extractall(tmpdir)

        # Aqui você processa os arquivos extraídos
        for root, _, files in os.walk(tmpdir):
            for file in files:
                # Os arquivos podem ter os formatos CSV, TXT, XLSX
                if file.endswith((".csv", ".txt", ".xlsx")):
                    caminho = os.path.join(root, file)

                    # TODO Realizar processamento necessário
                    print("Processando:", caminho)

##################################################################


if cod_status == 200:
    
    anos_disponiveis = extrair_anos_disponiveis(resp.text)
    
    # Ordenar e encontrar o ano mais recente
    anos_disponiveis.sort()
    ano_mais_recente = max(anos_disponiveis) if anos_disponiveis else None
    
    # Debug
    print(f"Anos disponíveis: {anos_disponiveis}")
    print(f"Ano mais recente disponível: {ano_mais_recente}")

    # Finalmente, listar os arquivos do ano mais recente que possuem a extenção .zip
    if ano_mais_recente:
        dir_ano_recente = f"{DIR_DEMONS_CONT}/{ano_mais_recente}/"
        resp_ano = requests.get(dir_ano_recente)
        
        if resp_ano.status_code == 200:
            soup_ano = BeautifulSoup(resp_ano.text, 'html.parser')
            arquivos = [link['href'] for link in soup_ano.find_all('a', href=True) if link['href'].endswith('.zip')]

            # Debug
            print(f"Arquivos disponíveis para o ano {ano_mais_recente}: {arquivos}")

            # Assumindo que os arquivos tenham o formato 1Tano.zip, 2Tano.zip, etc.
            # Crie uma lista com os nomes de arquivos dos últimos 3 trimestres
            # Se a lista contiver apenas 3 arquivos, inclua todos eles
            arquivos_trimestres = sorted(arquivos, reverse=True)[:3]
        else:
            print(f"Não foi possível acessar o diretório do ano {ano_mais_recente}. Status Code: {resp_ano.status_code}")

# print(DIR_DEMONS_CONT)