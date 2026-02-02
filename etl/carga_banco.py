import pandas as pd
import zipfile
import os
from sqlalchemy import create_engine, text
from sqlalchemy.types import Integer, String, Numeric
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASSWORD', 'postgres')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_HOST = "localhost"
DB_PORT = "5432"
DB_CONNECTION_STR = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

PASTA_RAW = "data/raw"
PASTA_PROCESSED = "data/processed"
ARQUIVO_CADOP = os.path.join(PASTA_RAW, "Relatorio_cadop.csv")
ARQUIVO_ZIP_DESPESAS = os.path.join(PASTA_PROCESSED, "consolidado_despesas.zip")
ARQUIVO_CSV_INTERNO_DESPESAS = "Relatorio_Final_Consolidado.csv"
ARQUIVO_AGREGADO = os.path.join(PASTA_PROCESSED, "despesas_agregadas.csv")

def get_engine():
    return create_engine(DB_CONNECTION_STR)

def carregar_dimensao_operadoras(engine):
    """
    Carrega a Tabela Dimensão (dim_operadoras).
    Contém dados cadastrais estáticos/lentos (SCD).
    """
    
    print("\n[1/3] Carregando Dimensão: dim_operadoras...")
    
    arq = ARQUIVO_CADOP
    if not os.path.exists(arq):
        arq = os.path.join(PASTA_RAW, "Relatorio_cadop_ativas.csv")
    
    try:
        df = pd.read_csv(arq, sep=';', encoding='ISO-8859-1', dtype=str)
    except:
        df = pd.read_csv(arq, sep=';', encoding='utf-8', dtype=str)

    col_map = {'Registro_ANS': 'reg_ans', 'REGISTRO_OPERADORA': 'reg_ans', 
               'CNPJ': 'cnpj', 'Razao_Social': 'razao_social', 'Modalidade': 'modalidade', 'UF': 'uf'}
    
    df = df.rename(columns=col_map)[list(col_map.values())[1:]] 
    df['reg_ans'] = pd.to_numeric(df['reg_ans'], errors='coerce')
    df.dropna(subset=['reg_ans'], inplace=True)
    
    df.drop_duplicates(subset=['reg_ans'], inplace=True)

    df.to_sql('dim_operadoras', engine, if_exists='replace', index=False, 
              dtype={'reg_ans': Integer(), 'cnpj': String(20), 'razao_social': String(255), 'uf': String(2)})
    
    # O Pandas 'to_sql' não cria Chaves Primárias ou Índices nativamente.
    # Executa SQL DDL manual para garantir integridade física e performance nos JOINs.
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE dim_operadoras ADD PRIMARY KEY (reg_ans);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_dim_cnpj ON dim_operadoras(cnpj);"))
        conn.commit()
    print("Índices e PK criados e salvos.")

def carregar_fato_despesas(engine):
    """
    Carrega a Tabela Fato (fato_despesas).
    Contém as transações financeiras e chaves estrangeiras.
    """
    
    print("\n[2/3] Carregando Fato: fato_despesas...")
    
    with zipfile.ZipFile(ARQUIVO_ZIP_DESPESAS, 'r') as z:
        with z.open(ARQUIVO_CSV_INTERNO_DESPESAS) as f:
            df = pd.read_csv(f, sep=';', encoding='utf-8')

    df.rename(columns={'CNPJ': 'cnpj_origem', 'RazaoSocial': 'razao_social_snapshot', 
                       'Trimestre': 'trimestre', 'Ano': 'ano', 'ValorDespesas': 'valor'}, inplace=True)
    
    df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0)
    
    # Trade-off:
    # Utilizei Numeric(15, 2) para 'valor' em vez de Float.
    # Isso evita erros de precisão em cálculos financeiros (ponto flutuante).
    df.to_sql('fato_despesas', engine, if_exists='replace', index=False,
              dtype={'valor': Numeric(15, 2), 'ano': Integer(), 'trimestre': Integer(), 'cnpj_origem': String(20)})
    
    # Índices criados nas colunas mais usadas em filtros (WHERE ano=X) 
    # e junções (JOIN on cnpj) para acelerar as queries analíticas.
    with engine.connect() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fato_cnpj ON fato_despesas(cnpj_origem);"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fato_tempo ON fato_despesas(ano, trimestre);"))
        conn.commit()
    print("Índices de performance criados e salvos.")

def carregar_agregados(engine):
    print("\n[3/3] Carregando Agregados...")
    if os.path.exists(ARQUIVO_AGREGADO):
        df = pd.read_csv(ARQUIVO_AGREGADO, sep=';', encoding='utf-8')
        df.columns = [c.lower() for c in df.columns]
        df.to_sql('agg_despesas', engine, if_exists='replace', index=False)

def main():
    print("Iniciando Carga Otimizada...")
    engine = get_engine()
    carregar_dimensao_operadoras(engine)
    carregar_fato_despesas(engine)
    carregar_agregados(engine)
    print("\nBanco Atualizado!")

if __name__ == "__main__":
    main()