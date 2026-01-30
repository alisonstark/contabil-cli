import os
import re
import tempfile
import zipfile

import pandas as pd
import requests
from bs4 import BeautifulSoup

"""
Funções auxiliares para processamento de dados de despesas e operadoras
Decidi por separar essas funções em um módulo próprio para manter o main.py mais limpo
e focado no fluxo principal do programa. Assim fica até mais fácil de manter e testar.

DOCUMENTAÇÃO DE MUDANÇAS:

1. FUNÇÃO ler_despesas():
   - Adicionado .round(2) após pd.to_numeric() para eliminar erros de precisão de ponto flutuante
   - Arredonda também a coluna DESPESA calculada
   - MOTIVO: Valores monetários não devem ter mais de 2 casas decimais; evita erros como "759558,3999999994"

2. FUNÇÃO correlacionar_despesas_com_operadoras():
   - Adicionados parâmetros opcionais: trimestre e ano
   - Adiciona automaticamente essas informações aos dicionários de resultado
   - MOTIVO: Permitir rastreabilidade de quando cada despesa ocorreu

3. FUNÇÃO consolidar_dados_em_csv():
   - Adicionado groupby() para agrupar por CNPJ, RazaoSocial, Trimestre e Ano
   - Soma todas as despesas para cada combinação (agregação)
   - MOTIVO: Eliminar duplicatas da mesma empresa no mesmo trimestre (múltiplos gastos → um total)
   - Alterado separador CSV para ';' (padrão brasileiro) e decimal para ','
   - MOTIVO: Compatibilidade com Excel em português
   - Adicionada validação de CNPJs antes de consolidar dados
   - MOTIVO: Garantir integridade dos dados no arquivo final

4. FUNÇÃO salvar_cnpjs_duplicados():
   - Alterado separador CSV para ';' e decimal para ',' (padrão brasileiro)
   - MOTIVO: Consistência com o arquivo principal

5. FUNÇÃO validar_cnpj():
   - Implementa algoritmo de validação de CNPJ (módulo 11)
   - Verifica formato (14 dígitos) e calcula dígitos verificadores
   - MOTIVO: Garantir integridade dos dados; CNPJs inválidos podem indicar problemas na fonte

6. FUNÇÃO filtrar_cnpjs_invalidos():
   - Separa registros com CNPJs válidos dos inválidos
   - Salva CNPJs inválidos em arquivo separado com CNPJ, REG_ANS e RazaoSocial
   - MOTIVO: Rastreabilidade de dados problemáticos para auditoria/correção
"""


def validar_cnpj(cnpj: str) -> bool:
    """
    Valida CNPJ usando algoritmo de módulo 11.
    Args:
        cnpj: String contendo o CNPJ (com ou sem formatação)
    Returns:
        True se CNPJ válido, False caso contrário
    """
    # Remove caracteres não numéricos
    cnpj_numeros = re.sub(r'\D', '', str(cnpj))
    
    # Verifica se tem 14 dígitos
    if len(cnpj_numeros) != 14:
        return False
    
    # Verifica se não são todos dígitos iguais (00000000000000, 11111111111111, etc)
    if cnpj_numeros == cnpj_numeros[0] * 14:
        return False
    
    # Calcula primeiro dígito verificador
    soma = 0
    peso = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    for i in range(12):
        soma += int(cnpj_numeros[i]) * peso[i]
    
    resto = soma % 11
    digito1 = 0 if resto < 2 else 11 - resto
    
    # Verifica primeiro dígito
    if int(cnpj_numeros[12]) != digito1:
        return False
    
    # Calcula segundo dígito verificador
    soma = 0
    peso = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    for i in range(13):
        soma += int(cnpj_numeros[i]) * peso[i]
    
    resto = soma % 11
    digito2 = 0 if resto < 2 else 11 - resto
    
    # Verifica segundo dígito
    if int(cnpj_numeros[13]) != digito2:
        return False
    
    return True


def filtrar_cnpjs_invalidos(dados_processados: list[dict], caminho_invalidos: str) -> list[dict]:
    """
    Filtra CNPJs inválidos e salva em arquivo separado.
    Args:
        dados_processados: Lista de dicionários com dados processados
        caminho_invalidos: Caminho do arquivo para salvar CNPJs inválidos
    Returns:
        Lista de dicionários apenas com CNPJs válidos
    """
    validos = []
    invalidos = []
    
    for registro in dados_processados:
        cnpj = str(registro.get('CNPJ', ''))
        
        if validar_cnpj(cnpj):
            validos.append(registro)
        else:
            # Salvar informações relevantes do registro inválido
            invalidos.append({
                'CNPJ': cnpj,
                'REG_ANS': registro.get('reg_ans', ''),
                'RazaoSocial': registro.get('RazaoSocial', '')
            })
    
    # Salvar CNPJs inválidos em arquivo separado
    if invalidos:
        df_invalidos = pd.DataFrame(invalidos)
        # Remover duplicatas - uma empresa só precisa aparecer uma vez
        df_invalidos = df_invalidos.drop_duplicates(subset=['CNPJ', 'REG_ANS', 'RazaoSocial'])
        df_invalidos.to_csv(caminho_invalidos, index=False, encoding='utf-8-sig', sep=';')
        print(f"Total de CNPJs inválidos únicos encontrados: {len(df_invalidos)}")
        print(f"CNPJs inválidos salvos em: {caminho_invalidos}")
    else:
        print("Nenhum CNPJ inválido encontrado.")
    
    print(f"Total de registros com CNPJs válidos: {len(validos)}")
    
    return validos


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

        # Converter colunas de valores monetários para numérico
        for col in ['VL_SALDO_FINAL', 'VL_SALDO_INICIAL']:
            # Substituir vírgula por ponto se necessário e converter para float
            df_filtered[col] = pd.to_numeric(
                df_filtered[col].astype(str).str.replace(',', '.'),
                errors='coerce'  # Valores inválidos viram NaN
            ).round(2)  # Arredondar para 2 casas decimais (valores monetários), do contrário pode dar erro de precisão

        # Calcular despesa uma única vez
        df_filtered['DESPESA'] = (df_filtered['VL_SALDO_FINAL'] - df_filtered['VL_SALDO_INICIAL']).round(2)

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

    """
    resultados = []

    # MUDANÇA 1: Busca dinâmica por coluna de registro ANS em despesas
    # MOTIVO: Diferentes fontes usam nomes diferentes (REG_ANS, REGISTRO_OPERADORA, etc)
    # ANTES: coluna_reg_ans = 'REG_ANS' (hardcoded, falhava com outros nomes)
    # DEPOIS: Tenta múltiplos nomes até encontrar um válido
    coluna_reg_ans_despesas = None
    possiveis_nomes_despesas = ['REGISTRO_OPERADORA', 'REG_ANS', 'CD_REGISTRO_ANS', 'REGISTRO_ANS', 'CD_REG_ANS']

    # Procurar o nome correto da coluna em despesas
    for nome in possiveis_nomes_despesas:
        if nome in df_despesas.columns:
            coluna_reg_ans_despesas = nome
            break

    if not coluna_reg_ans_despesas:
        # Potencialmente pode ajudar a atualizar o código se novos nomes forem encontrados
        print(f"AVISO: Nenhuma coluna de registro ANS encontrada em despesas. Colunas disponíveis: {df_despesas.columns.tolist()}")
        return []

    # Realizar a mesma busca para operadoras
    coluna_reg_ans_operadoras = None
    possiveis_nomes_operadoras = ['REG_ANS', 'REGISTRO_ANS', 'CD_REGISTRO_ANS', 'REGISTRO_OPERADORA']

    for nome in possiveis_nomes_operadoras:
        if nome in df_operadoras.columns:
            coluna_reg_ans_operadoras = nome
            break

    if not coluna_reg_ans_operadoras:
        print(f"AVISO: Nenhuma coluna de registro ANS encontrada em operadoras. Colunas disponíveis: {df_operadoras.columns.tolist()}")
        return []

    # O mesmo para CNPJ e Razão Social, apesar de que esses nomes são mais padronizados nos arquivos que abri manualmente
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

    # TODO OTIMIZAÇÃO: Usar .to_dict('records') em vez de iterrows()
    # MOTIVO: iterrows() é MUITO lento (O(n²)), .to_dict() é vetorizado e muito mais rápido
    # ANTES: for _, row in df_merged.iterrows(): ... (670k iterações = lento!)
    # DEPOIS: Converte em lista de dicts diretamente (operação C, super rápida)
    df_resultado = df_merged[[coluna_reg_ans_operadoras, coluna_cnpj, coluna_razao, 'DESPESA']].copy()
    df_resultado.columns = ['reg_ans', 'CNPJ', 'RazaoSocial', 'ValorDespesas']
    resultados = df_resultado.to_dict('records')

    # Debug
    print(f"Total de correlações encontradas: {len(resultados)}")

    return resultados


# Função principal: correlacionar despesas com operadoras
def correlacionar_despesas_com_operadoras(caminho_arquivo: str, df_operadoras: pd.DataFrame, trimestre: int = None, ano: int = None) -> list[dict]:
    """
    Processa arquivo de despesas e correlaciona com informações de operadoras.
    Args:
        caminho_arquivo: Caminho do arquivo de despesas (CSV, TXT ou XLSX)
        df_operadoras: DataFrame com dados de operadoras (já carregado)
        trimestre: Número do trimestre (1-4)
        ano: Ano dos dados
    Returns:
        Lista de dicionários com despesas correlacionadas
    """
    try:
        # Ler despesas
        df_despesas = ler_despesas(caminho_arquivo)

        # Correlacionar dados
        resultados = correlacionar_dados(df_despesas, df_operadoras)
        
        # Adicionar informações de trimestre e ano se fornecidas
        if trimestre is not None or ano is not None:
            for resultado in resultados:
                if trimestre is not None:
                    resultado['Trimestre'] = trimestre
                if ano is not None:
                    resultado['Ano'] = ano

        return resultados

    except Exception as e:
        print(f"Erro ao correlacionar despesas com operadoras: {e}")
        return []


# Criar uma função para baixar e extrair arquivos zip, processando-os antes de serem excluídos
def extrair_e_processar_zip(url_zip, df_operadoras: pd.DataFrame) -> list[dict]:
    """
    Baixa e extrai arquivos de um arquivo zip da URL fornecida e processa-os.
    Args:
        url_zip: URL do arquivo zip
        df_operadoras: DataFrame com dados de operadoras
    Returns:
        list: Lista de resultados processados de todos os arquivos
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
        # IMPORTANTE detalhe que descobri depois de alguns thrown errors: Se movêssemos a lógica pra fora deste bloco 'with',
        # os arquivos já teriam sido deletados pelo Python, e aí não haveria o que processar
        for root, _, files in os.walk(tmpdir):
            for file in files:
                if file.endswith((".csv", ".txt", ".xlsx")):
                    caminho = os.path.join(root, file)
                    print("Processando:", caminho)

                    # Processar arquivo enquanto ainda existe (dentro do contexto 'with')
                    resultados = correlacionar_despesas_com_operadoras(caminho, df_operadoras)
                    # Acumular resultados; a função .extend adiciona todos os itens da lista
                    todos_resultados.extend(resultados)
        # Retornar todos os resultados processados, em vez de caminhos de arquivos (já que eles serão deletados)
        return todos_resultados
    
# Criar uma função que salve em um arquivo CSV CNPJs duplicados com razões sociais diferentes
def salvar_cnpjs_duplicados(dados_processados: list[dict], caminho_saida: str):
    """
    Salva CNPJs duplicados com razões sociais diferentes em um arquivo CSV.
    Args:
        dados_processados: Lista de dicionários com dados processados
        caminho_saida: Caminho do arquivo CSV de saída
    """
    try:
        df = pd.DataFrame(dados_processados)

        # Agrupar por CNPJ e contar razões sociais únicas
        duplicados = df.groupby('CNPJ').filter(lambda x: x['RazaoSocial'].nunique() > 1)

        if not duplicados.empty:
            resultados = []

            for cnpj, grupo in duplicados.groupby('CNPJ'):
                razoes = grupo['RazaoSocial'].unique()
                despesas = grupo['ValorDespesas'].values
                anos = grupo['Ano'].values
                trimestres = grupo['Trimestre'].values

                for i in range(len(razoes)):
                    for j in range(i + 1, len(razoes)):
                        resultados.append({
                            'CNPJ': cnpj,
                            'RazaoSocial1': razoes[i],
                            'RazaoSocial2': razoes[j],
                            'Trimestre': trimestres[i],
                            'Ano': anos[i],
                            'ValorDespesas1': despesas[i],
                            'ValorDespesas2': despesas[j],
                        })

            df_duplicados = pd.DataFrame(resultados)
            df_duplicados.to_csv(caminho_saida, index=False, encoding='utf-8-sig', sep=';', decimal=',')
            print(f"CNPJs duplicados salvos em: {caminho_saida}")
        else:
            print("Nenhum CNPJ duplicado encontrado.")

    except Exception as e:
        print(f"Erro ao salvar CNPJs duplicados: {e}")

# Criar uma função para consolidar os dados processados dos últimos 3 trimestres em um único arquivo CSV
# A função deve se certificar de que o arquivo CSV terá apenas as colunas: CNPJ , RazaoSocial , Trimestre , Ano , ValorDespesas
def consolidar_dados_em_csv(dados_processados: list[dict], caminho_saida: str):
    """
    Consolida os dados processados em um único arquivo CSV.
    Args:
        dados_processados: Lista de dicionários com dados processados
        caminho_saida: Caminho do arquivo CSV de saída
    """
    try:
        # Filtrar CNPJs inválidos ANTES de processar
        caminho_invalidos = caminho_saida.replace('.csv', '_cnpjs_invalidos.csv')
        dados_validos = filtrar_cnpjs_invalidos(dados_processados, caminho_invalidos)
        
        df_consolidado = pd.DataFrame(dados_validos)

        # Verificar se o DataFrame não está vazio antes de salvar
        if not df_consolidado.empty:
            colunas_desejadas = ["CNPJ", "RazaoSocial", "Trimestre", "Ano", "ValorDespesas"]
            
            # Verificar quais colunas existem e selecionar apenas essas
            colunas_existentes = [col for col in colunas_desejadas if col in df_consolidado.columns]
            
            if len(colunas_existentes) != len(colunas_desejadas):
                colunas_faltantes = [col for col in colunas_desejadas if col not in df_consolidado.columns]
                print(f"AVISO: Colunas faltantes no DataFrame: {colunas_faltantes}")
                print(f"Colunas disponíveis: {df_consolidado.columns.tolist()}")
            
            df_consolidado = df_consolidado[colunas_existentes]
            
            # Arredondar coluna de valores para 2 casas decimais (valores monetários)
            if 'ValorDespesas' in df_consolidado.columns:
                df_consolidado['ValorDespesas'] = df_consolidado['ValorDespesas'].astype(float).round(2)
            
            # Agrupar por CNPJ, RazaoSocial, Trimestre e Ano, somando as despesas
            # Isso garante que se uma empresa teve múltiplas despesas no mesmo trimestre,
            # teremos apenas uma linha com o total agregado
            df_consolidado = df_consolidado.groupby(['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano'], as_index=False).agg({
                'ValorDespesas': 'sum'
            })
            
            # Arredondar novamente após a soma
            df_consolidado['ValorDespesas'] = df_consolidado['ValorDespesas'].astype(float).round(2)
            
            # Aproveitar para criar o CSV com CNPJs duplicados com razões sociais diferentes
            salvar_cnpjs_duplicados(dados_processados, caminho_saida.replace('.csv', '_cnpjs_duplicados.csv'))
            # Salvar com separador ';' que é padrão no Brasil e compatível com Excel
            df_consolidado.to_csv(caminho_saida, index=False, encoding='utf-8-sig', sep=';', decimal=',')
            print(f"Dados consolidados salvos em: {caminho_saida}")
        else:
            print("Nenhum dado para consolidar.")

    except KeyError as e:
        print(f"Erro ao consolidar dados em CSV: Coluna não encontrada - {e}")
        if dados_processados:
            df = pd.DataFrame(dados_processados)
            print(f"Colunas disponíveis: {df.columns.tolist()}")
    except Exception as e:
        print(f"Erro ao consolidar dados em CSV: {e}")


