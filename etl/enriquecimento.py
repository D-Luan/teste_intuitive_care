import pandas as pd
import requests
import os
import re
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

URL_CADOP = "https://dadosabertos.ans.gov.br/FTP/PDA/operadoras_de_plano_de_saude_ativas/Relatorio_cadop.csv"
PASTA_RAW = "data/raw"
PASTA_PROCESSED = "data/processed"

ARQUIVO_CADOP = os.path.join(PASTA_RAW, "Relatorio_cadop_ativas.csv")
ARQUIVO_VALIDADO = os.path.join(PASTA_PROCESSED, "dados_validados.csv")
ARQUIVO_ENRIQUECIDO = os.path.join(PASTA_PROCESSED, "dados_enriquecidos.csv")

def limpar_cnpj_join(cnpj):
    """
    Padroniza CNPJs para garantir o match no Join.
    Remove pontuação e espaços. Retorna apenas dígitos.
    """
    if pd.isna(cnpj) or cnpj == "":
        return ""
    
    limpo = re.sub(r'[^0-9]', '', str(cnpj))
    return limpo

def baixar_cadop_ativas():
    """Baixa o CSV atualizado de Operadoras Ativas."""

    print("Verificando arquivo Cadop Ativas...")
    if os.path.exists(ARQUIVO_CADOP):
        print("Arquivo já existe. Pulando download.")
        return

    print("Baixando da ANS...")
    try:
        r = requests.get(URL_CADOP, verify=False)
        if r.status_code == 200:
            with open(ARQUIVO_CADOP, 'wb') as f:
                f.write(r.content)
            print("Download concluído.")
        else:
            raise Exception(f"Erro {r.status_code} ao baixar Cadop.")
    except Exception as e:
        print(f"Erro de conexão: {e}")
        raise e

def executar_enriquecimento():
    """
    Pipeline de Enriquecimento de Dados.
    Executa o cruzamento (Join) entre Transacional 
    (Despesas) e Mestre (Cadastro).
    """
    
    print("Iniciando Enriquecimento de Dados...")
    
    baixar_cadop_ativas()

    if not os.path.exists(ARQUIVO_VALIDADO):
        raise FileNotFoundError(f"Arquivo validado não encontrado: {ARQUIVO_VALIDADO}")
    
    print("Carregando tabelas...")
    df_despesas = pd.read_csv(ARQUIVO_VALIDADO, sep=';', encoding='utf-8', dtype=str)
    
    # Encoding ISO-8859-1 é padrão em arquivos governamentais antigos
    df_cadop = pd.read_csv(ARQUIVO_CADOP, sep=';', encoding='ISO-8859-1', dtype=str)

    print("Normalizando chaves de CNPJ...")

    # Essa normalização garante que diferenças de 
    # formatação (ponto/traço) não impeçam o cruzamento dos dados.
    df_despesas['CNPJ_KEY'] = df_despesas['CNPJ'].apply(limpar_cnpj_join)
    df_cadop['CNPJ_KEY'] = df_cadop['CNPJ'].apply(limpar_cnpj_join)

    qtd_cadop_antes = len(df_cadop)

    # O cadastro pode conter CNPJs duplicados. 
    # Remove duplicatas mantendo a primeira ocorrência para evitar 
    # multiplicação de linhas durante o Join.
    df_cadop = df_cadop.drop_duplicates(subset=['CNPJ_KEY'], keep='first')
    print(f"Cadop deduplicado: {qtd_cadop_antes} com {len(df_cadop)} registros.")
    
    col_reg = next((c for c in df_cadop.columns if 'REGISTRO' in c.upper()), 'RegistroANS')
    
    cols_cadop = df_cadop[[ 'CNPJ_KEY', col_reg, 'Modalidade', 'UF' ]].copy()
    cols_cadop.rename(columns={col_reg: 'RegistroANS_Novo'}, inplace=True)

    print("Executando Left Join via CNPJ...")

    # Trade-off:
    # Optei por LEFT JOIN. A prioridade é manter o registro financeiro (Despesa),
    # mesmo que a operadora não seja encontrada no cadastro ativo.
    df_final = pd.merge(df_despesas, cols_cadop, on='CNPJ_KEY', how='left')

    # Tratamento de perda no Join
    # Preenche metadados faltantes para manter consistência no banco de dados
    cols_novas = ['RegistroANS_Novo', 'Modalidade', 'UF']
    for col in cols_novas:
        df_final[col] = df_final[col].fillna("DESCONHECIDO")

    df_final.drop(columns=['CNPJ_KEY'], inplace=True)

    df_final.to_csv(ARQUIVO_ENRIQUECIDO, index=False, sep=';', encoding='utf-8')
    print(f"\nEnriquecimento concluído: {ARQUIVO_ENRIQUECIDO}")
    print(f"Total de Linhas: {len(df_final)} (Deve ser igual à entrada: {len(df_despesas)})")

if __name__ == "__main__":
    executar_enriquecimento()