import os
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def buscar_zips_recursivamente(url_atual, profundidade_max=2, nivel_atual=0):
    prefixo_log = "   " * (nivel_atual + 1)
    
    if nivel_atual == 0:
        print(f"{prefixo_log} Vasculhando: {url_atual}")

    headers = {'User-Agent': 'Mozilla/5.0'}
    zips_encontrados = []

    if nivel_atual >= profundidade_max:
        return []
    
    try:
        response = requests.get(url_atual, headers=headers, verify=False)
        if response.status_code != 200:
            return []
            
        soup = BeautifulSoup(response.text, 'html.parser')
        
        for link in soup.find_all('a', href=True):
            href = link['href']
            url_completa = urljoin(url_atual, href)

            if '.zip' in href.lower() and re.search(r'202\d', href):
                    print(f"{prefixo_log} -> ZIP encontrado: {href}")
                    zips_encontrados.append(url_completa)
            
            elif href.endswith('/'):
                if href.startswith('?') or href in ['../', './', '/'] or href.startswith('/'):
                    continue

                zips_internos = buscar_zips_recursivamente(url_completa, profundidade_max, nivel_atual + 1)
                zips_encontrados.extend(zips_internos)

        return zips_encontrados
    
    except Exception as e:
        print(f"Erro ao ler {url_atual}: {e}")
        return []

def buscar_links_demonstracoes(url_raiz):
    candidatos_totais = []

    ano_atual = datetime.now().year
    anos_alvo = [f"{ano_atual - i}/" for i in range(3)]

    for ano in anos_alvo:
        url_ano = urljoin(url_raiz, ano)

        zips = buscar_zips_recursivamente(url_ano)
        candidatos_totais.extend(zips)

    candidatos_unicos = sorted(list(set(candidatos_totais)), reverse=True)

    print(f"Total de arquivos válidos encontrados: {len(candidatos_totais)}")
    return candidatos_unicos[:3]

def baixar_arquivo(url, pasta_destino):
    nome_arquivo = url.split('/')[-1]
    caminho_final = os.path.join(pasta_destino, nome_arquivo)

    if os.path.exists(caminho_final):
        print(f"Arquivo já existe: {nome_arquivo}")
        return True
    
    print(f"Baixando: {nome_arquivo}...")

    try:
        r = requests.get(url, stream=True, verify=False)

        if r.status_code == 200:
            with open(caminho_final, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        
            print(f"Download concluído: {nome_arquivo}")
            return True
        else:
            print(f"Erro {r.status_code} ao baixar {url}")
            return False
        
    except Exception as e:
        print(f"Erro de conexão: {e}")
        return False
    
def main():
    PASTA_RAW = "data/raw"
    os.makedirs(PASTA_RAW, exist_ok=True)

    print("ETAPA 1: Demonstrações Contábeis")
    URL_CONTABEIS = "https://dadosabertos.ans.gov.br/FTP/PDA/demonstracoes_contabeis/"

    links = buscar_links_demonstracoes(URL_CONTABEIS)

    if not links:
        raise Exception("Nenhum arquivo ZIP encontrado.")
    
    print(f"Arquivos selecionados para download: {[u.split('/')[-1] for u in links]}")

    for url in links:
            baixar_arquivo(url, PASTA_RAW)

    nomes_arquivos = [u.split('/')[-1] for u in links]
    print(f"Arquivos selecionados: {nomes_arquivos}")

    print("\nETAPA 2: Dados Cadastrais (Cadop)")
    
    URL_CADOP = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
    
    sucesso_cadop = baixar_arquivo(URL_CADOP, PASTA_RAW)

    if not sucesso_cadop:
        raise Exception(f"Falha no download do Cadop: {URL_CADOP}")

    print("\nProcesso de Download Finalizado!")

if __name__ == "__main__":
    main()