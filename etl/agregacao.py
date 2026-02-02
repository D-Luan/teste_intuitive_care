import pandas as pd
import zipfile
import os

PASTA_PROCESSED = "data/processed"
ARQUIVO_ENTRADA = os.path.join(PASTA_PROCESSED, "dados_enriquecidos.csv")
ARQUIVO_SAIDA_CSV = os.path.join(PASTA_PROCESSED, "despesas_agregadas.csv")

NOME_ZIP_FINAL = "teste_luan_nascimento.zip" 
ARQUIVO_SAIDA_ZIP = os.path.join(PASTA_PROCESSED, NOME_ZIP_FINAL)

def calcular_estatisticas_operadora(df):
    """Calcula KPIs: Total, Média Trimestral e Desvio Padrão."""
    
    df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'], errors='coerce').fillna(0)

    # Primeiro soma as despesas por Trimestre. Isso é crucial para que a 
    # 'Média' calculada posteriormente seja a "Média dos Trimestres",
    # e não a média isolada de cada lançamento contábil.
    df_trimestral = df.groupby(['RazaoSocial', 'UF', 'Ano', 'Trimestre'])['ValorDespesas'].sum().reset_index()
    
    df_trimestral.rename(columns={'ValorDespesas': 'TotalTrimestre'}, inplace=True)

    df_agregado = df_trimestral.groupby(['RazaoSocial', 'UF']).agg(
        Valor_Total=('TotalTrimestre', 'sum'),
        Media_Trimestral=('TotalTrimestre', 'mean'),
        Desvio_Padrao=('TotalTrimestre', 'std')
    ).reset_index()

    # O desvio padrão retorna NaN se houver apenas uma amostra (um único trimestre).
    # Assume 0.0 (sem variação) para manter a consistência numérica.
    df_agregado['Desvio_Padrao'] = df_agregado['Desvio_Padrao'].fillna(0.0)

    cols_numericas = ['Valor_Total', 'Media_Trimestral', 'Desvio_Padrao']
    df_agregado[cols_numericas] = df_agregado[cols_numericas].round(2)

    # Trade-off:
    # Utilizei o QuickSort padrão do Pandas (em memória).
    # Dado o volume de dados após a agregação (apenas 1 linha por operadora/UF),
    # a ordenação em memória é extremamente performática e não justifica indexação externa.
    df_agregado.sort_values(by='Valor_Total', ascending=False, inplace=True)

    return df_agregado

def executar_agregacao():
    print("Iniciando Agregação e Estatística...")

    if not os.path.exists(ARQUIVO_ENTRADA):
        raise FileNotFoundError(f"Arquivo de entrada não encontrado: {ARQUIVO_ENTRADA}")

    print("Carregando dataset...")
    df = pd.read_csv(ARQUIVO_ENTRADA, sep=';', encoding='utf-8')

    print("Calculando estatísticas...")
    df_final = calcular_estatisticas_operadora(df)

    df_final.to_csv(ARQUIVO_SAIDA_CSV, index=False, sep=';', encoding='utf-8')
    print(f"CSV Agregado salvo: {ARQUIVO_SAIDA_CSV}")

    print(f"Gerando entrega final: {NOME_ZIP_FINAL}...")
    with zipfile.ZipFile(ARQUIVO_SAIDA_ZIP, 'w', zipfile.ZIP_DEFLATED) as zf:
        zf.write(ARQUIVO_SAIDA_CSV, arcname="despesas_agregadas.csv")
    
    print(f"\nProcesso concluído: {ARQUIVO_SAIDA_ZIP}")
    print(f"Operadora com maior despesa: {df_final.iloc[0]['RazaoSocial']} - R$ {df_final.iloc[0]['Valor_Total']}")

if __name__ == "__main__":
    executar_agregacao()