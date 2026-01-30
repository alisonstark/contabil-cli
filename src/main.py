import requests
from bs4 import BeautifulSoup
import pandas as pd

from processamento_dados import (
    extrair_e_processar_zip,
    baixar_operadoras,
    extrair_anos_disponiveis,
    consolidar_dados_em_csv,
)

# Pra começar, vamos importar a url base da API
URL_BASE = "https://dadosabertos.ans.gov.br/FTP/PDA/"

# Recuperar o diretório de demonstrações contábeis, assumindo que ele tenha o nome "demonstracoes_contabeis"
DIR_DEMONS_CONT = f"{URL_BASE}/demonstracoes_contabeis"

# Se certifique de que a resposta do servidor foi OK
resp = requests.get(DIR_DEMONS_CONT)
cod_status = resp.status_code

# Debug
print(f"Status Code: {cod_status}")
print(f"Response Text: {resp.text[:500]}")

# Todo o processamento dentro do bloco if só será possível se o status code for 200
dados_processados = []

# Processar apenas se a requisição foi bem sucedida
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
                
                # Extrair trimestre e ano do nome do arquivo (formato: 1T2024.zip, 2T2024.zip, etc)
                import re
                match = re.search(r'(\d)T(\d{4})', arquivo)
                trimestre = int(match.group(1)) if match else None
                ano_arquivo = int(match.group(2)) if match else ano_mais_recente
                
                resultados = extrair_e_processar_zip(url_arquivo, df_operadoras)
                
                # OTIMIZAÇÃO: Não criar DataFrame para cada resultado
                # ANTES: print(pd.DataFrame([resultado])) para cada item (muito lento)
                # DEPOIS: Imprimir diretamente com dict ou criar um único DataFrame com todos
                print(f"\nResultados para {arquivo}:")
                if resultados:
                    # Adicionar trimestre e ano a cada resultado
                    for resultado in resultados:
                        if trimestre:
                            resultado['Trimestre'] = trimestre
                        resultado['Ano'] = ano_arquivo
                    
                    # Salvar os dados processados na lista de dicionários para uso futuro
                    dados_processados.extend(resultados)
                    # Criar um único DataFrame com todos os primeiros 10 resultados (para fins de amostra)
                    df_amostra = pd.DataFrame(resultados[:10])
                    print(df_amostra.to_string())
                else:
                    print("Nenhum resultado encontrado.")
                
                print(f"Total de registros processados: {len(resultados)}")
                print("-" * 80)
            
            # Após processar todos os arquivos, consolidar os dados em um CSV, salvando no diretório dados_consolidados
            caminho_saida = "dados_consolidados/demonstracoes_contabeis_consolidadas.csv"
            consolidar_dados_em_csv(dados_processados, caminho_saida)
            
        else:
            print(f"Não foi possível acessar o diretório do ano {ano_mais_recente}. Código de Status: {resp_ano.status_code}")
else:
    print(f"Não foi possível acessar o diretório de demonstrações contábeis. Código de Status: {cod_status}")
