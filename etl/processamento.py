import os
import zipfile
import pandas as pd
import re
import io

PASTA_RAW = "data/raw"
PASTA_PROCESSED = "data/processed"
ARQUIVO_SAIDA = os.path.join(PASTA_PROCESSED, "demonstracoes_consolidadas.csv")

def listar_zips_raw(pasta):
    """
    Retorna lista ordenada de zips para garantir 
    processamento sequencial.
    """
    
    return sorted([
        os.path.join(pasta, f) 
        for f in os.listdir(pasta) 
        if f.lower().endswith('.zip')
    ])

def extrair_metadados_nome(nome_arquivo):
    """
    Extrai Trimestre e Ano do nome do arquivo 
    (Ex: '1T2023').
    Importante porque algumas colunas internas dos arquivos 
    não trazem a data.
    """
    
    match = re.search(r'(\d)T(\d{4})', nome_arquivo.upper())
    if match:
        return match.group(1), match.group(2)
    return None, None

def limpar_valor_monetario(valor):
    """
    Normaliza valores monetários mistos (PT-BR 
    com vírgula vs US com ponto).
    Remove pontos de milhar e substitui vírgula decimal 
    por ponto.
    Retorna 0.0 em caso de falha de conversão para manter 
    integridade da coluna numérica.
    """
    
    if pd.isna(valor): return 0.0
    
    val_str = str(valor).strip()
    
    if val_str.replace('.', '', 1).isdigit(): return float(val_str)
    if ',' in val_str:
        limpo = val_str.replace('.', '').replace(',', '.')
        try: return float(limpo)
        except ValueError: return 0.0
    return 0.0

def carregar_dataframe_robusto(arquivo_aberto, nome_arquivo):
    """
    Usa decodificadores para garantir a leitura sem 
    travar o pipeline, evitando formatos inconsistentes
    (CSV, TXT, XLSX) e encodings variados.
    """
    
    extensao = nome_arquivo.lower().split('.')[-1]
    
    if extensao == 'xlsx':
        try: 
            return pd.read_excel(arquivo_aberto, dtype=str)
        except Exception as e: 
            print(f"Erro Excel: {e}"); 
            return None

    if extensao in ['csv', 'txt']:
        try:
            arquivo_aberto.seek(0)
            return pd.read_csv(arquivo_aberto, sep=';', encoding='utf-8', dtype=str)
        except:
            pass
            
        try:
            arquivo_aberto.seek(0)
            return pd.read_csv(arquivo_aberto, sep=',', encoding='utf-8', dtype=str)
        except:
            pass

        try:
            arquivo_aberto.seek(0)
            return pd.read_csv(arquivo_aberto, sep=';', encoding='ISO-8859-1', dtype=str)
        except:
            pass
            
    return None

def processar_arquivo_individual(caminho_zip, primeiro_arquivo):
    """
    Processa um único ZIP e escreve incrementalmente no 
    CSV final.
    """
    
    nome_zip = os.path.basename(caminho_zip)
    trimestre, ano = extrair_metadados_nome(nome_zip)

    if not trimestre or not ano:
        return False

    try:
        with zipfile.ZipFile(caminho_zip, 'r') as z:
            candidatos = [f for f in z.namelist() if f.lower().endswith(('.csv', '.txt', '.xlsx'))]
            if not candidatos: return False

            nome_arquivo_interno = candidatos[0]
            with z.open(nome_arquivo_interno) as f:
                df = carregar_dataframe_robusto(f, nome_arquivo_interno)
                
                if df is None or df.empty: return False

                col_descricao = next((c for c in df.columns if 'DESCRICAO' in c.upper()), None)
                if not col_descricao: return False

                # Filtra apenas linhas contendo "EVENTO" ou "SINISTRO" para isolar 
                # as despesas assistenciais diretas, ignorando outras receitas/despesas operacionais.
                termos = ["EVENTO", "SINISTRO"]
                filtro = df[col_descricao].astype(str).str.upper().str.contains('|'.join(termos), na=False)
                df_filtrado = df[filtro].copy()

                if df_filtrado.empty: return False

                df_filtrado['trimestre'] = trimestre
                df_filtrado['ano'] = ano
                
                col_valor = next((c for c in df.columns if 'VALOR' in c.upper() or 'VL_' in c.upper()), None)
                if not col_valor: return False
                
                df_filtrado['valor'] = df_filtrado[col_valor].apply(limpar_valor_monetario)

                cols_map = {col_descricao: 'descricao', 'REG_ANS': 'reg_ans', 'DATA': 'data_contabil'}
                df_filtrado.rename(columns={k:v for k,v in cols_map.items() if k in df_filtrado.columns}, inplace=True)
                
                for col in ['data_contabil', 'reg_ans']:
                    if col not in df_filtrado.columns: df_filtrado[col] = None

                df_final = df_filtrado[['trimestre', 'ano', 'data_contabil', 'reg_ans', 'descricao', 'valor']]

                # Trade-off (Processamento Incremental):
                # Utilizei mode='a' (append) para escrever no disco à medida que processamos.
                # Isso mantém o consumo de memória baixo e constante, independente do volume total de dados.
                modo = 'w' if primeiro_arquivo else 'a'
                header = primeiro_arquivo
                df_final.to_csv(ARQUIVO_SAIDA, index=False, mode=modo, header=header, encoding='utf-8')
                
                print(f"Processado: {nome_zip} ({len(df_final)} linhas)")
                return True

    except Exception as e:
        print(f"Erro em {nome_zip}: {e}")
        return False

def main():
    os.makedirs(PASTA_PROCESSED, exist_ok=True)
    if os.path.exists(ARQUIVO_SAIDA): os.remove(ARQUIVO_SAIDA)

    zips = listar_zips_raw(PASTA_RAW)
    if not zips: raise Exception("Sem arquivos.")

    periodos_processados = set()
    primeiro = True

    print(f"Iniciando processamento de {len(zips)} arquivos...")
    
    for zip_file in zips:
        nome = os.path.basename(zip_file)
        t, a = extrair_metadados_nome(nome)
        
        chave = f"{a}-{t}"
        if chave in periodos_processados:
            print(f"Ignorando duplicata de período: {nome} ({t}º Trimestre {a})")
            continue
            
        sucesso = processar_arquivo_individual(zip_file, primeiro)
        if sucesso:
            periodos_processados.add(chave)
            primeiro = False

    print("\nProcessamento concluído!")

if __name__ == "__main__":
    main()