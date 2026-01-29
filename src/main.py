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
# print(f"Response Text: {resp.text[:500]}")

"""
DOCUMENTAÇÃO DE MUDANÇAS:

1. FUNÇÃO ler_despesas():
   - PROBLEMA: Colunas VL_SALDO_FINAL e VL_SALDO_INICIAL eram lidas como strings
   - SOLUÇÃO: Convertidas para numérico com pd.to_numeric(), tratando vírgulas como separador decimal
   - MOTIVO: Arquivos brasileiros usam vírgula (1.234,56), não ponto (1,234.56)
   - RESULTADO: Agora é possível fazer operações matemáticas (+, -, *, /) com esses valores

2. FUNÇÃO correlacionar_dados():
   - PROBLEMA: Código buscava hardcoded a coluna 'REG_ANS', mas diferentes fontes usam nomes diferentes
   - SOLUÇÃO: Busca dinâmica por múltiplos nomes possíveis em ambos os DataFrames
   - MOTIVO: Arquivo de despesas usava 'REGISTRO_OPERADORA', arquivo de operadoras usava 'REG_ANS'
   - RESULTADO: Código agora adapta-se automaticamente a nomes diferentes de colunas

3. FUNÇÃO baixar_e_extrair_zip():
   - PROBLEMA: Arquivos temporários eram excluídos ao sair do bloco 'with', antes de serem lidos
   - SOLUÇÃO: Processamento dos arquivos DENTRO do contexto temporário (antes que sejam deletados)
   - MOTIVO: TemporaryDirectory() limpa tudo ao finalizar, deixando caminhos inválidos
   - RESULTADO: Arquivos são lidos enquanto ainda existem, retornando resultados já processados

4. CÓDIGO PRINCIPAL:
   - Adicionado debug para exibir colunas reais de cada arquivo
   - Simplificado o loop principal para receber resultados já processados
"""


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
    Suporta formatos: CSV, TXT e XLSX
    Args:
        caminho_arquivo: Caminho do arquivo de despesas
    Returns:
        DataFrame com despesas filtradas
    Raises:
        ValueError: Se formato de arquivo não é suportado
        Exception: Se houver erro ao ler o arquivo
    
    MUDANÇAS IMPLEMENTADAS:
    - Adição de conversão numérica para colunas de valores monetários
    - Tratamento de formato brasileiro (vírgula como separador decimal)
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
        
        # MUDANÇA: Converter colunas de valores para numérico (tratando vírgulas como separador decimal)
        # MOTIVO: Arquivos brasileiros usam vírgula (1.234,56) como separador decimal
        # ANTES: df_filtered['VL_SALDO_FINAL'] era string, causando erro "unsupported operand type(s) for -: 'str' and 'str'"
        # DEPOIS: Convertidas para float, permitindo operações matemáticas
        for col in ['VL_SALDO_FINAL', 'VL_SALDO_INICIAL']:
            # Substituir vírgula por ponto se necessário e converter para float
            df_filtered[col] = pd.to_numeric(
                df_filtered[col].astype(str).str.replace(',', '.'), 
                errors='coerce'  # Valores inválidos viram NaN
            )
        
        # Calcular despesa uma única vez
        df_filtered['DESPESA'] = df_filtered['VL_SALDO_FINAL'] - df_filtered['VL_SALDO_INICIAL']
        
        # Debug: mostrar colunas disponíveis
        print(f"Colunas disponíveis no arquivo: {df_filtered.columns.tolist()}")
        
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
        df_despesas: DataFrame com dados de despesas (coluna de ID da operadora)
        df_operadoras: DataFrame com dados de operadoras (ID, CNPJ, Razao_Social)
    Returns:
        Lista de dicionários com despesas correlacionadas
    
    MUDANÇAS IMPLEMENTADAS:
    - Busca dinâmica por nomes de colunas em vez de hardcoded
    - Funciona com diferentes nomes: REG_ANS, REGISTRO_OPERADORA, REGISTRO_ANS, etc
    - Encontra automaticamente colunas de CNPJ e Razão Social
    - Exibe mensagens de debug sobre quais colunas foram encontradas
    """
    resultados = []
    
    # MUDANÇA 1: Busca dinâmica por coluna de registro ANS em despesas
    # MOTIVO: Diferentes fontes usam nomes diferentes (REG_ANS, REGISTRO_OPERADORA, etc)
    # ANTES: coluna_reg_ans = 'REG_ANS' (hardcoded, falhava com outros nomes)
    # DEPOIS: Tenta múltiplos nomes até encontrar um válido
    coluna_reg_ans_despesas = None
    possiveis_nomes_despesas = ['REGISTRO_OPERADORA', 'REG_ANS', 'CD_REGISTRO_ANS', 'REGISTRO_ANS', 'CD_REG_ANS']
    
    for nome in possiveis_nomes_despesas:
        if nome in df_despesas.columns:
            coluna_reg_ans_despesas = nome
            break
    
    if not coluna_reg_ans_despesas:
        print(f"AVISO: Nenhuma coluna de registro ANS encontrada em despesas. Colunas disponíveis: {df_despesas.columns.tolist()}")
        return []
    
    # MUDANÇA 2: Busca dinâmica por coluna de registro ANS em operadoras
    coluna_reg_ans_operadoras = None
    possiveis_nomes_operadoras = ['REG_ANS', 'REGISTRO_ANS', 'CD_REGISTRO_ANS', 'REGISTRO_OPERADORA']
    
    for nome in possiveis_nomes_operadoras:
        if nome in df_operadoras.columns:
            coluna_reg_ans_operadoras = nome
            break
    
    if not coluna_reg_ans_operadoras:
        print(f"AVISO: Nenhuma coluna de registro ANS encontrada em operadoras. Colunas disponíveis: {df_operadoras.columns.tolist()}")
        return []
    
    # MUDANÇA 3: Busca dinâmica por colunas de CNPJ e Razão Social
    # MOTIVO: Diferentes arquivos podem ter nomes diferentes para as mesmas informações
    coluna_cnpj = 'CNPJ' if 'CNPJ' in df_operadoras.columns else 'CD_CNPJ' if 'CD_CNPJ' in df_operadoras.columns else None
    coluna_razao = 'Razao_Social' if 'Razao_Social' in df_operadoras.columns else 'NM_RAZAO_SOCIAL' if 'NM_RAZAO_SOCIAL' in df_operadoras.columns else None
    
    if not coluna_cnpj or not coluna_razao:
        print(f"AVISO: Colunas CNPJ ou Razão Social não encontradas. Colunas disponíveis: {df_operadoras.columns.tolist()}")
        return []
    
    print(f"Usando colunas: despesas['{coluna_reg_ans_despesas}'] <-> operadoras['{coluna_reg_ans_operadoras}']")
    print(f"CNPJ: '{coluna_cnpj}', Razão Social: '{coluna_razao}'")
    
    # Fazer merge entre despesas e operadoras
    df_merged = df_despesas.merge(
        df_operadoras[[coluna_reg_ans_operadoras, coluna_cnpj, coluna_razao]], 
        left_on=coluna_reg_ans_despesas,
        right_on=coluna_reg_ans_operadoras,
        how='inner'
    )
    
    # OTIMIZAÇÃO: Usar .to_dict('records') em vez de iterrows()
    # MOTIVO: iterrows() é MUITO lento (O(n²)), .to_dict() é vetorizado e muito mais rápido
    # ANTES: for _, row in df_merged.iterrows(): ... (670k iterações = lento!)
    # DEPOIS: Converte em lista de dicts diretamente (operação C, super rápida)
    df_resultado = df_merged[[coluna_reg_ans_operadoras, coluna_cnpj, coluna_razao, 'DESPESA']].copy()
    df_resultado.columns = ['reg_ans', 'cnpj', 'razao_social', 'despesa']
    resultados = df_resultado.to_dict('records')
    
    print(f"Total de correlações encontradas: {len(resultados)}")
    
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


# Criar uma função para baixar e extrair arquivos zip, processando-os antes de serem excluídos
def baixar_e_extrair_zip(url_zip, df_operadoras: pd.DataFrame) -> list[dict]:
    """
    Baixa e extrai arquivos de um arquivo zip da URL fornecida e processa-os.
    Args:
        url_zip: URL do arquivo zip
        df_operadoras: DataFrame com dados de operadoras
    Returns:
        list: Lista de resultados processados de todos os arquivos
    
    MUDANÇAS IMPLEMENTADAS:
    - Processamento de arquivos DENTRO do contexto temporário (com tempfile.TemporaryDirectory())
    - Retorna resultados já processados em vez de caminhos de arquivos
    - Evita o erro: "arquivo não pode ser lido após sair do contexto temporário"
    
    PROBLEMA ORIGINAL:
    - Com 'with tempfile.TemporaryDirectory() as tmpdir:', ao sair do bloco,
      Python automaticamente deleta todos os arquivos e diretórios
    - Isso causava IOError ao tentar ler arquivos que não existiam mais
    
    SOLUÇÃO:
    - Correlacionar_despesas_com_operadoras() é chamada DENTRO do bloco 'with'
    - Resultados são armazenados em 'todos_resultados' antes de sair do contexto
    - Apenas os resultados (dados) são retornados, não os caminhos dos arquivos
    """

    todos_resultados = []

    with tempfile.TemporaryDirectory() as tmpdir:
        caminho_do_zip = os.path.join(tmpdir, "arquivo.zip")

        with requests.get(url_zip, stream=True) as r:
            r.raise_for_status()
            with open(caminho_do_zip, "wb") as f:
                for chunk in r.iter_content(8192):
                    f.write(chunk)

        with zipfile.ZipFile(caminho_do_zip, "r") as z:
            z.extractall(tmpdir)

        # Processar os arquivos extraídos DENTRO do contexto temporário
        # IMPORTANTE: Se movéssemos a lógica para fora deste bloco 'with',
        # os arquivos já teriam sido deletados pelo Python
        for root, _, files in os.walk(tmpdir):
            for file in files:
                # Os arquivos podem ter os formatos CSV, TXT, XLSX
                if file.endswith((".csv", ".txt", ".xlsx")):
                    caminho = os.path.join(root, file)
                    print("Processando:", caminho)
                    
                    # Processar arquivo enquanto ainda existe (dentro do contexto 'with')
                    resultados = correlacionar_despesas_com_operadoras(caminho, df_operadoras)
                    todos_resultados.extend(resultados)

        return todos_resultados

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

    # Finalmente, listar os arquivos do ano mais recente que possuem a extensão .zip
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
            print(f"Colunas do arquivo de operadoras: {df_operadoras.columns.tolist()}")
            
            # Para cada arquivo dos trimestres, baixe, extraia e processe 
            for arquivo in arquivos_trimestres:
                url_arquivo = f"{dir_ano_recente}{arquivo}"
                print(f"Baixando e extraindo: {url_arquivo}")
                resultados = baixar_e_extrair_zip(url_arquivo, df_operadoras)
                
                # OTIMIZAÇÃO: Não criar DataFrame para cada resultado
                # ANTES: print(pd.DataFrame([resultado])) para cada item (muito lento)
                # DEPOIS: Imprimir diretamente com dict ou criar um único DataFrame com todos
                print(f"\nResultados para {arquivo}:")
                if resultados:
                    # Criar um único DataFrame com todos os primeiros 10 resultados (muito mais rápido)
                    df_amostra = pd.DataFrame(resultados[:10])
                    print(df_amostra.to_string())
                else:
                    print("Nenhum resultado encontrado.")
                
                print(f"Total de registros processados: {len(resultados)}")
                print("-" * 80)
        else:
            print(f"Não foi possível acessar o diretório do ano {ano_mais_recente}. Código de Status: {resp_ano.status_code}")
