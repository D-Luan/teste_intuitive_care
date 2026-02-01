import pandas as pd
import zipfile
import io
import os
import re

CAMINHO_ZIP_ENTRADA = "data/processed/consolidado_despesas.zip"
ARQUIVO_CSV_INTERNO = "Relatorio_Final_Consolidado.csv"
CAMINHO_CSV_SAIDA = "data/processed/dados_validados.csv"

def validar_cnpj_matematico(cnpj):
    """
    Valida um CNPJ utilizando o algoritmo oficial de 
    Módulo 11.
    """
    
    # Limpeza preventiva para garantir que apenas números sejam validados
    cnpj = re.sub(r'[^0-9]', '', str(cnpj))

    if len(cnpj) != 14:
        return False
    
    if len(set(cnpj)) == 1:
        return False

    # Validação do Primeiro Dígito
    pesos1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma1 = sum(int(cnpj[i]) * pesos1[i] for i in range(12))
    resto1 = soma1 % 11
    digito1 = 0 if resto1 < 2 else 11 - resto1

    if int(cnpj[12]) != digito1:
        return False

    # Validação do Segundo Dígito
    pesos2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    soma2 = sum(int(cnpj[i]) * pesos2[i] for i in range(13))
    resto2 = soma2 % 11
    digito2 = 0 if resto2 < 2 else 11 - resto2

    if int(cnpj[13]) != digito2:
        return False

    return True

def executar_validacao():
    """
    Executa o pipeline de Data Quality.
    Aplica regras de validação (CNPJ, Valor, Razão Social) 
    e gera flags de auditoria.
    """
    
    print("Iniciando Validação de Dados...")

    if not os.path.exists(CAMINHO_ZIP_ENTRADA):
        raise Exception(f"Arquivo de entrada não encontrado: {CAMINHO_ZIP_ENTRADA}")

    try:
        with zipfile.ZipFile(CAMINHO_ZIP_ENTRADA, 'r') as z:
            with z.open(ARQUIVO_CSV_INTERNO) as f:
                df = pd.read_csv(f, sep=';', encoding='utf-8', dtype={'CNPJ': str})
                print(f"Carregado: {len(df)} registros.")
    except Exception as e:
        print(f"Erro ao abrir ZIP: {e}")
        return

    print("Aplicando regras de validação...")

    # Razão Social não pode ser vazia
    df['check_razao'] = df['RazaoSocial'].notna() & (df['RazaoSocial'] != '')
    
    # Valores devem ser numéricos e positivos
    df['ValorDespesas'] = pd.to_numeric(df['ValorDespesas'], errors='coerce').fillna(0)
    df['check_valor'] = df['ValorDespesas'] > 0

    # CNPJ deve ser matematicamente válido
    df['check_cnpj'] = df['CNPJ'].apply(validar_cnpj_matematico)

    def definir_status(row):
        erros = []
        if not row['check_cnpj']: erros.append("CNPJ_INVALIDO")
        if not row['check_valor']: erros.append("VALOR_INVALIDO")
        if not row['check_razao']: erros.append("RAZAO_VAZIA")
        
        return "OK" if not erros else "; ".join(erros)

    # Trade-off:
    # Em vez de excluir os registros inválidos,
    # optei por mantê-los e adicionar uma coluna de flag 'status_auditoria'.
    # Isso permite que a área de negócios corrija os dados na origem posteriormente.
    df['status_auditoria'] = df.apply(definir_status, axis=1)
    
    qtd_invalidos = len(df[df['status_auditoria'] != 'OK'])
    print("\nRelatório de Qualidade:")
    print(f"Válidos: {len(df) - qtd_invalidos}")
    print(f"Com Inconsistências: {qtd_invalidos}")
    print(f"CNPJs Inválidos: {len(df[~df['check_cnpj']])}")
    print(f"Valores Zerados/Negativos: {len(df[~df['check_valor']])}")

    df.to_csv(CAMINHO_CSV_SAIDA, index=False, sep=';', encoding='utf-8')
    print(f"\nArquivo salvo em: {CAMINHO_CSV_SAIDA}")

if __name__ == "__main__":
    executar_validacao()