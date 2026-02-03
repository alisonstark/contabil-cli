import os
import re

import requests
from bs4 import BeautifulSoup
import pandas as pd

from processamento_dados import (
    extrair_e_processar_zip,
    baixar_operadoras,
    extrair_anos_disponiveis,
    consolidar_dados_em_csv,
    juntar_dados_com_operadoras
)

def main() -> None:
    # URLs base
    url_base = "https://dadosabertos.ans.gov.br/FTP/PDA/"
    dir_demons_cont = f"{url_base}/demonstracoes_contabeis"

    # Buscar diretório principal
    resp = requests.get(dir_demons_cont)
    cod_status = resp.status_code

    # Debug
    # print(f"Status Code: {cod_status}")
    # print(f"Response Text: {resp.text[:500]}")

    if cod_status != 200:
        print(f"Não foi possível acessar o diretório de demonstrações contábeis. Código de Status: {cod_status}")
        return

    # Extrair anos disponíveis e selecionar o mais recente
    anos_disponiveis = extrair_anos_disponiveis(resp.text)
    anos_disponiveis.sort()
    ano_mais_recente = max(anos_disponiveis) if anos_disponiveis else None

    # Debug
    print(f"Anos disponíveis: {anos_disponiveis}")
    print(f"Ano mais recente disponível: {ano_mais_recente}")

    if not ano_mais_recente:
        print("Nenhum ano disponível encontrado.")
        return

    # Listar arquivos do ano mais recente
    dir_ano_recente = f"{dir_demons_cont}/{ano_mais_recente}/"
    resp_ano = requests.get(dir_ano_recente)

    if resp_ano.status_code != 200:
        print(f"Não foi possível acessar o diretório do ano {ano_mais_recente}. Código de Status: {resp_ano.status_code}")
        return

    soup_ano = BeautifulSoup(resp_ano.text, 'html.parser')
    arquivos = [link['href'] for link in soup_ano.find_all('a', href=True) if link['href'].endswith('.zip')]

    # Debug
    print(f"Arquivos disponíveis para o ano {ano_mais_recente}: {arquivos}")

    # Selecionar os últimos 3 trimestres
    arquivos_trimestres = sorted(arquivos, reverse=True)[:3]

    # Baixar operadoras UMA VEZ antes do loop (tive que errar pra entender o básico)
    operadoras_url = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
    caminho_operadoras = baixar_operadoras(operadoras_url)
    df_operadoras = pd.read_csv(caminho_operadoras, encoding='latin1', sep=';')

    # Limpeza de memória: deletar arquivo temporário após criar o DataFrame
    # isso evita manter arquivos desnecessários no disco
    os.unlink(caminho_operadoras)

    # Debug
    print(f"Operadoras carregadas. Total de operadoras: {len(df_operadoras)}")
    print(f"Colunas do arquivo de operadoras: {df_operadoras.columns.tolist()}")

    # Processar arquivos trimestrais
    dados_processados: list[dict] = []

    for arquivo in arquivos_trimestres:
        url_arquivo = f"{dir_ano_recente}{arquivo}"
        print(f"Baixando e extraindo: {url_arquivo}")

        # Extrair trimestre e ano do nome do arquivo (formato: 1T2024.zip, 2T2024.zip, etc)
        match = re.search(r'(\d)T(\d{4})', arquivo)
        trimestre = int(match.group(1)) if match else None
        ano_arquivo = int(match.group(2)) if match else ano_mais_recente

        resultados = extrair_e_processar_zip(url_arquivo, df_operadoras)

        print(f"\nResultados para {arquivo}:")
        if resultados:
            # Adicionar trimestre e ano a cada resultado
            for resultado in resultados:
                if trimestre:
                    resultado['Trimestre'] = trimestre
                resultado['Ano'] = ano_arquivo

            # Acumular resultados
            dados_processados.extend(resultados)

            # Amostra para inspeção
            df_amostra = pd.DataFrame(resultados[:10])
            print(df_amostra.to_string())
        else:
            print("Nenhum resultado encontrado.")

        print(f"Total de registros processados: {len(resultados)}")
        print("-" * 80)

    # Consolidar dados e gerar CSV final
    caminho_saida = "dados_consolidados/demonstracoes_contabeis_consolidadas.csv"
    consolidar_dados_em_csv(dados_processados, caminho_saida)

    # Fazer join dos dados consolidados com operadoras
    juntar_dados_com_operadoras(caminho_saida, df_operadoras)


if __name__ == "__main__":
    main()
