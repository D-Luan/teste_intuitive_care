import os
import zipfile
import pandas as pd
import re
import io

PASTA_RAW = "data/raw"
PASTA_PROCESSED = "data/processed"
ARQUIVO_SAIDA = os.path.join(PASTA_PROCESSED, "demonstracoes_consolidadas.csv")

MAPA_COLUNAS = {
    'DATA': 'data_contabil',
    'REG_ANS': 'reg_ans',
    'CD_CONTA_CONTABIL': 'conta',
    'DESCRICAO': 'descricao',
    'VL_SALDO_FINAL': 'valor'
}

def listar_zips_raw(pasta):
    return sorted([
        os.path.join(pasta, f) 
        for f in os.listdir(pasta) 
        if f.lower().endswith('.zip')
    ])

def extrair_metadados_nome(nome_arquivo):
    match = re.search(r'(\d)T(\d{4})', nome_arquivo.upper())
    if match:
        return match.group(1), match.group(2)
    return None, None

def limpar_valor_monetario(valor):
    if pd.isna(valor):
        return 0.0
    
    val_str = str(valor).strip()
    
    if val_str.replace('.', '', 1).isdigit(): 
        return float(val_str)

    if ',' in val_str:
        limpo = val_str.replace('.', '').replace(',', '.')
        try:
            return float(limpo)
        except ValueError:
            return 0.0
            
    return 0.0

def carregar_dataframe_robusto(arquivo_aberto, nome_arquivo):
    extensao = nome_arquivo.lower().split('.')[-1]
    
    if extensao == 'xlsx':
        try:
            return pd.read_excel(arquivo_aberto, dtype=str)
        except Exception as e:
            print(f"Erro ao ler Excel: {e}")
            return None

    if extensao in ['csv', 'txt']:
        try:
            arquivo_aberto.seek(0)
            return pd.read_csv(arquivo_aberto, sep=';', encoding='ISO-8859-1', dtype=str)
        except Exception:
            pass

        try:
            arquivo_aberto.seek(0)
            return pd.read_csv(arquivo_aberto, sep=',', encoding='utf-8', dtype=str)
        except Exception:
            pass
            
    print(f"Formato não suportado ou ilegível: {nome_arquivo}")
    return None

def processar_arquivo_individual(caminho_zip, primeiro_arquivo):
    nome_zip = os.path.basename(caminho_zip)
    print(f"Processando: {nome_zip}...")

    trimestre, ano = extrair_metadados_nome(nome_zip)

    if not trimestre or not ano:
        print(f"Ignorado: Não foi possível identificar Ano/Trimestre no nome {nome_zip}")
        return

    try:
        with zipfile.ZipFile(caminho_zip, 'r') as z:
            candidatos = [f for f in z.namelist() if f.lower().endswith(('.csv', '.txt', '.xlsx'))]
            
            if not candidatos:
                print(f"Ignorado: {nome_zip}")
                return

            nome_arquivo_interno = candidatos[0]
            
            with z.open(nome_arquivo_interno) as f:
                df = carregar_dataframe_robusto(f, nome_arquivo_interno)
                
                if df is None or df.empty:
                    return

                col_descricao = None
                for col in df.columns:
                    if 'DESCRICAO' in col.upper():
                        col_descricao = col
                        break
                
                if not col_descricao:
                    print("Coluna de Descrição não encontrada.")
                    return

                termos_interesse = ["EVENTO", "SINISTRO"]
                filtro = df[col_descricao].astype(str).str.upper().str.contains('|'.join(termos_interesse), na=False)
                df_filtrado = df[filtro].copy()

                if df_filtrado.empty:
                    print("Sem dados de Eventos/Sinistros.")
                    return

                df_filtrado['trimestre'] = trimestre
                df_filtrado['ano'] = ano

                col_valor = [c for c in df.columns if 'VALOR' in c.upper() or 'VL_' in c.upper()][0]
                df_filtrado['valor'] = df_filtrado[col_valor].apply(limpar_valor_monetario)

                cols_finais = {
                    col_descricao: 'descricao',
                    'REG_ANS': 'reg_ans',
                    'DATA': 'data_contabil'
                }
                df_filtrado.rename(columns=cols_finais, inplace=True)

                colunas_saida = ['trimestre', 'ano', 'data_contabil', 'reg_ans', 'descricao', 'valor']
                
                for col in colunas_saida:
                    if col not in df_filtrado.columns:
                        df_filtrado[col] = None

                df_final = df_filtrado[colunas_saida]

                modo = 'w' if primeiro_arquivo else 'a'
                header = primeiro_arquivo
                df_final.to_csv(ARQUIVO_SAIDA, index=False, mode=modo, header=header, encoding='utf-8')
                
                print(f"-> Finalizado: {len(df_final)} linhas. (Ref: {trimestre}T{ano})\n")

    except Exception as e:
        print(f"Erro crítico em {nome_zip}: {e}")
        raise e

def main():
    os.makedirs(PASTA_PROCESSED, exist_ok=True)

    if os.path.exists(ARQUIVO_SAIDA):
        os.remove(ARQUIVO_SAIDA)

    zips = listar_zips_raw(PASTA_RAW)

    if not zips:
        raise Exception("Sem arquivos.")

    print(f"Iniciando processamento de {len(zips)} arquivos...\n")
    
    for i, zip_file in enumerate(zips):
        processar_arquivo_individual(zip_file, (i == 0))

    print("\nProcessamento Concluído!")

if __name__ == "__main__":
    main()