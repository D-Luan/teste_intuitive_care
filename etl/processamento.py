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
    Aceita negativos e formatos variados (BR/US).
    """
    if pd.isna(valor): return 0.0
    val_str = str(valor).strip()
    
    if ',' in val_str:
        limpo = val_str.replace('.', '').replace(',', '.')
        try: return float(limpo)
        except: pass
        
    try:
        return float(val_str)
    except:
        pass
        
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

                col_conta = next((c for c in df.columns if 'CONTA' in c.upper() or 'CD_CONTA' in c.upper()), None)
                col_descricao = next((c for c in df.columns if 'DESCRICAO' in c.upper()), None)
                
                # Regra de Negócio: Filtragem Hierárquica
                # A conta 411 (Eventos Conhecidos) é usada para evitar a duplicidade de valores
                # gerada pela soma das contas sintéticas (pai) e analíticas (filhas).
                if col_conta:
                    df['conta_limpa'] = df[col_conta].astype(str).str.replace('.', '', regex=False).str.strip()
                    filtro = df['conta_limpa'] == '411'
                else:
                    print(f"Aviso: Filtrando por texto em {nome_arquivo_interno}")
                    filtro = df[col_descricao].str.upper().str.contains("EVENTOS/SINISTROS CONHECIDOS OU AVISADOS DE ASSISTÊNCIA À SAÚDE", na=False)

                df_filtrado = df[filtro].copy()

                if df_filtrado.empty: 
                    if col_conta:
                         filtro_alt = df['conta_limpa'].str.startswith('411') & (df['conta_limpa'].str.len() == 3)
                         df_filtrado = df[filtro_alt].copy()
                    
                    if df_filtrado.empty: return False

                df_filtrado['trimestre'] = trimestre
                df_filtrado['ano'] = ano
                
                # Seleção de Coluna de Valor
                # Priorizei VL_SALDO_FINAL pois o VL_SALDO_INICIAL frequentemente vem zerado
                # no 1º Trimestre, o que causaria perda de dados de Jan/Fev/Mar.
                col_final = next((c for c in df.columns if 'VL_SALDO_FINAL' in c.upper()), None)
                if col_final:
                    col_valor = col_final
                else:
                    col_valor = next((c for c in df.columns if 'VALOR' in c.upper() or 'VL_' in c.upper()), None)
                if not col_valor: return False
                
                df_filtrado['valor'] = df_filtrado[col_valor].apply(limpar_valor_monetario)

                cols_map = {col_descricao: 'descricao', 'REG_ANS': 'reg_ans', 'DATA': 'data_contabil'}
                df_filtrado.rename(columns={k:v for k,v in cols_map.items() if k in df_filtrado.columns}, inplace=True)
                
                if not col_descricao: col_descricao = next((c for c in df.columns if 'DESCRICAO' in c.upper()), 'descricao')
                cols_map = {col_descricao: 'descricao', 'REG_ANS': 'reg_ans', 'DATA': 'data_contabil'}
                df_filtrado.rename(columns={k:v for k,v in cols_map.items() if k in df_filtrado.columns}, inplace=True)

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