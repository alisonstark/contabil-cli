import requests
from bs4 import BeautifulSoup
import re
import pandas as pd

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
def extrair_anos_disponiveis(html_text) -> list[int]:
    """
    Extrai anos disponíveis do HTML fornecido.
    Args:
        html_text: HTML da página do diretório de demonstrações contábeis
    Returns:
        Lista de anos disponíveis como inteiros
    """

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


# Baixar arquivo de operadoras e retornar caminho temporário
def baixar_operadoras(url: str) -> str:
    """
    Baixa arquivo de operadoras de plano de saúde da URL fornecida.
    Args:
        url: URL do arquivo de operadoras
    Returns:
        Caminho do arquivo temporário com as operadoras
    Raises:
        requests.RequestException: Se falhar ao baixar o arquivo
    """
    try:
        resp_operadoras = requests.get(url, timeout=10)
        resp_operadoras.raise_for_status()
    except requests.RequestException as e:
        print(f"Erro ao baixar arquivo de operadoras: {e}")
        raise
    
    # Salvar o conteúdo em um arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as tmpfile:
        tmpfile.write(resp_operadoras.content)

        # Debug
        print(f"Arquivo de operadoras baixado para: {tmpfile.name}")
        return tmpfile.name


# Ler arquivo de despesas de acordo com seu formato
def ler_despesas(caminho_arquivo: str) -> pd.DataFrame:
    """
    Lê arquivo de despesas e retorna DataFrame filtrado.
    Suporta formatos: CSV, TXT (TSV) e XLSX
    Args:
        caminho_arquivo: Caminho do arquivo de despesas
    Returns:
        DataFrame com despesas filtradas
    Raises:
        ValueError: Se formato de arquivo não é suportado
        Exception: Se houver erro ao ler o arquivo
    """
    try:
        if caminho_arquivo.endswith('.csv'):
            df = pd.read_csv(caminho_arquivo, encoding='latin1', sep=';')
        elif caminho_arquivo.endswith('.txt'):
            df = pd.read_csv(caminho_arquivo, encoding='latin1', sep='\t')
        elif caminho_arquivo.endswith('.xlsx'):
            df = pd.read_excel(caminho_arquivo)
        else:
            raise ValueError(f"Formato de arquivo não suportado: {caminho_arquivo}")
        
        # Filtrar despesas: excluir linhas com 'Despesas com Eventos / Sinistros'
        df_filtered = df[df['DESCRICAO'] != 'Despesas com Eventos / Sinistros'].copy()
        
        # Calcular despesa uma única vez
        df_filtered['DESPESA'] = df_filtered['VL_SALDO_FINAL'] - df_filtered['VL_SALDO_INICIAL']
        
        return df_filtered
        
    except KeyError as e:
        print(f"Erro: Coluna não encontrada no arquivo. {e}")
        raise
    except pd.errors.ParserError as e:
        print(f"Erro ao fazer parsing do arquivo: {e}")
        raise
    except IOError as e:
        print(f"Erro ao ler arquivo: {e}")
        raise
    except Exception as e:
        print(f"Erro ao processar arquivo: {e}")
        raise


# Correlacionar dados de despesas com operadoras
# Usando DataFrames para merge eficiente
def correlacionar_dados(df_despesas: pd.DataFrame, df_operadoras: pd.DataFrame) -> list[dict]:
    """
    Faz merge entre despesas e informações de operadoras.
    Args:
        df_despesas: DataFrame com dados de despesas (deve ter coluna REG_ANS)
        df_operadoras: DataFrame com dados de operadoras (deve ter REG_ANS, CNPJ, Razao_Social)
    Returns:
        Lista de dicionários com despesas correlacionadas
    """
    resultados = []
    
    # Fazer merge entre despesas e operadoras com base na coluna REG_ANS
    df_merged = df_despesas.merge(
        df_operadoras[['REG_ANS', 'CNPJ', 'Razao_Social']], 
        on='REG_ANS', 
        how='inner'
    )
    
    # Construir resultado a partir do merge
    for _, row in df_merged.iterrows():
        resultado = {
            "reg_ans": row['REG_ANS'],
            "cnpj": row['CNPJ'],
            "razao_social": row['Razao_Social'],
            "despesa": row['DESPESA']
        }
        resultados.append(resultado)
        # Debug
        print(f"Correlacionado: {resultado}")
    
    return resultados


# Função principal: correlacionar despesas com operadoras
def correlacionar_despesas_com_operadoras(caminho_arquivo: str, df_operadoras: pd.DataFrame) -> list[dict]:
    """
    Processa arquivo de despesas e correlaciona com informações de operadoras.
    Args:
        caminho_arquivo: Caminho do arquivo de despesas (CSV, TXT ou XLSX)
        df_operadoras: DataFrame com dados de operadoras (já carregado)
    Returns:
        Lista de dicionários com despesas correlacionadas
    """
    try:
        # Ler despesas
        df_despesas = ler_despesas(caminho_arquivo)
        
        # Correlacionar dados
        resultados = correlacionar_dados(df_despesas, df_operadoras)
        
        return resultados
        
    except Exception as e:
        print(f"Erro ao correlacionar despesas com operadoras: {e}")
        return []


# Criar uma função para baixar e extrair arquivos zip, retornando os arquivos extraídos
def baixar_e_extrair_zip(url_zip) -> list:
    """
    Baixa e extrai arquivos de um arquivo zip da URL fornecida.
    Args:
        url_zip: URL do arquivo zip
    Returns:
        list: Lista de caminhos dos arquivos extraídos
    """

    arquivos_extraidos = []

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
                    arquivos_extraidos.append(caminho)

                    # TODO Realizar processamento necessário
                    print("Processando:", caminho)

        return arquivos_extraidos

################### Fim das funções auxiliares ###################
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
            
            # Baixar operadoras UMA VEZ antes do loop
            operadoras_url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
            caminho_operadoras = baixar_operadoras(operadoras_url)
            df_operadoras = pd.read_csv(caminho_operadoras, encoding='latin1', sep=';')

            # Debug
            print(f"Operadoras carregadas. Total de operadoras: {len(df_operadoras)}")
            
            # Para cada arquivo dos trimestres, baixe, extraia e processe 
            for arquivo in arquivos_trimestres:
                url_arquivo = f"{dir_ano_recente}{arquivo}"
                print(f"Baixando e extraindo: {url_arquivo}")
                arquivos_extraidos = baixar_e_extrair_zip(url_arquivo)
                
                for caminho_arquivo in arquivos_extraidos:
                    resultados = correlacionar_despesas_com_operadoras(caminho_arquivo, df_operadoras)
                    print(f"Resultados para o arquivo {caminho_arquivo}:")
                    for resultado in resultados:
                        print(resultado)
        else:
            print(f"Não foi possível acessar o diretório do ano {ano_mais_recente}. Status Code: {resp_ano.status_code}")
