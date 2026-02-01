import pandas as pd
import os
import zipfile

PASTA_RAW = "data/raw"
PASTA_PROCESSED = "data/processed"
ARQUIVO_DESPESAS = os.path.join(PASTA_PROCESSED, "demonstracoes_consolidadas.csv")
ARQUIVO_CADOP = os.path.join(PASTA_RAW, "Relatorio_cadop.csv")
ARQUIVO_FINAL_CSV = os.path.join(PASTA_PROCESSED, "relatorio_final.csv")
ARQUIVO_FINAL_ZIP = os.path.join(PASTA_PROCESSED, "consolidado_despesas.zip")

def carregar_cadop():
    """
    Carrega e prepara os dados cadastrais das operadoras.
    Resolve inconsistências de duplicidade de registro.
    """ 
    
    print("Carregando Cadop...")
    
    try:
        df = pd.read_csv(ARQUIVO_CADOP, sep=';', encoding='ISO-8859-1', dtype=str)
        
        colunas_necessarias = {
            'REGISTRO_OPERADORA': 'reg_ans', 
            'CNPJ': 'CNPJ', 
            'Razao_Social': 'RazaoSocial'
        }
        
        df = df[list(colunas_necessarias.keys())].rename(columns=colunas_necessarias)
        
        # Garante que a chave de join seja numérica para 
        # bater com o arquivo de despesas 
        df['reg_ans'] = pd.to_numeric(df['reg_ans'], errors='coerce')
        
        # Trade-off:
        # O arquivo de cadastro pode conter registros 
        # históricos ou duplicados para o mesmo Reg_ANS.
        # Optei por manter apenas a primeira ocorrência 
        # para garantir uma relação 1:N limpa no Join. 
        df.drop_duplicates(subset=['reg_ans'], inplace=True)
        
        print(f"Cadop carregado: {len(df)} operadoras.")
        return df
    
    except Exception as e:
        print(f"Erro ao ler Cadop: {e}")
        raise e

def processar_consolidacao():
    """
    Executa o Join entre Despesas e Dados Cadastrais e
    gera o artefato final.
    Trata inconsistências de valores e falhas de 
    relacionamento. 
    """
    
    print("\nIniciando Consolidação dos Dados...")
    
    if not os.path.exists(ARQUIVO_DESPESAS):
        raise Exception(f"Arquivo de despesas não encontrado: {ARQUIVO_DESPESAS}. Rode a etapa 1.2 antes.")

    df_despesas = pd.read_csv(ARQUIVO_DESPESAS, dtype={'reg_ans': int, 'valor': float})
    print(f"Despesas carregadas: {len(df_despesas)} linhas.")

    df_cadop = carregar_cadop()

    # Trade-off - Estratégia de Join:
    # Utilizei LEFT JOIN para priorizar a integridade dos dados financeiros (Despesas).
    # Se uma operadora não for encontrada no cadastro ativo,
    # mantêm o registro financeiro e marca os dados cadastrais como desconhecidos. 
    df_final = pd.merge(df_despesas, df_cadop, on='reg_ans', how='left')

    # Remove valores zerados ou negativos que não agregam valor analítico. 
    qtd_antes = len(df_final)
    df_final = df_final[df_final['valor'] != 0]
    qtd_zeros = qtd_antes - len(df_final)

    print(f"Limpeza: {qtd_zeros} linhas com valor 0.0 removidas.")
    
    # Tratamento de Falhas no Join:
    # Preenchimento de valores nulos gerados pelo Left Join 
    # para operadoras não encontradas. 
    df_final['RazaoSocial'] = df_final['RazaoSocial'].fillna(f"DESCONHECIDA")
    df_final['CNPJ'] = df_final['CNPJ'].fillna("00000000000000")

    df_final.rename(columns={'valor': 'ValorDespesas', 'trimestre': 'Trimestre', 'ano': 'Ano'}, inplace=True)

    colunas_saida = ['CNPJ', 'RazaoSocial', 'Trimestre', 'Ano', 'ValorDespesas']
    
    for col in colunas_saida:
        if col not in df_final.columns:
            df_final[col] = None

    # Exportação em UTF-8 com ; para compatibilidade com Excel brasileiro 
    df_export = df_final[colunas_saida]

    print("\nGerando artefatos finais...")
    
    df_export.to_csv(ARQUIVO_FINAL_CSV, index=False, encoding='utf-8', sep=';')
    
    with zipfile.ZipFile(ARQUIVO_FINAL_ZIP, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(ARQUIVO_FINAL_CSV, arcname="Relatorio_Final_Consolidado.csv")
    
    print(f"ZIP Criado: {ARQUIVO_FINAL_ZIP}")
    
    if os.path.exists(ARQUIVO_FINAL_CSV):
        os.remove(ARQUIVO_FINAL_CSV)
    
    print("\nConsolidação e Análise Concluídos com Sucesso!")

if __name__ == "__main__":
    processar_consolidacao()