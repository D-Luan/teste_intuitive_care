import pytest
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASSWORD', 'postgres')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_HOST = "localhost"
DB_CONNECTION_STR = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}'

@pytest.fixture(scope="module")
def db_engine():
    """
    Fixture com escopo de módulo.
    Cria a conexão apenas uma vez para todos os testes, 
    economizando overhead.
    """
    engine = create_engine(DB_CONNECTION_STR)
    yield engine
    engine.dispose()

def test_query_1_crescimento(db_engine):
    sql = """
    WITH despesas_trimestrais AS (
        SELECT d.razao_social, f.ano, f.trimestre, SUM(f.valor) as total_trimestre
        FROM fato_despesas f
        JOIN dim_operadoras d ON f.cnpj_origem = d.cnpj
        GROUP BY d.razao_social, f.ano, f.trimestre
    ),
    limites_temporais AS (
        SELECT DISTINCT razao_social,
            FIRST_VALUE(total_trimestre) OVER (PARTITION BY razao_social ORDER BY ano ASC, trimestre ASC) as valor_inicial,
            FIRST_VALUE(total_trimestre) OVER (PARTITION BY razao_social ORDER BY ano DESC, trimestre DESC) as valor_final
        FROM despesas_trimestrais
    )
    SELECT razao_social, valor_inicial, valor_final,
        ROUND(((valor_final - valor_inicial) / NULLIF(valor_inicial, 0)) * 100, 2) as crescimento_pct
    FROM limites_temporais
    WHERE valor_inicial > 0
    ORDER BY crescimento_pct DESC
    LIMIT 5;
    """
    with db_engine.connect() as conn:
        result = conn.execute(text(sql)).fetchall()
        
        # Garante que o ETL populou o banco corretamente
        assert len(result) > 0, "Query 1 não retornou resultados!"
        assert len(result) <= 5, "Query 1 retornou mais que o limite de 5!"
        
        print("\nResultado da Query 1: os 5 maiores crescimentos")
        for row in result:
            print(f"Operadora: {row[0]} | Crescimento: {row[3]}%")

def test_query_2_estados(db_engine):
    sql = """
    SELECT d.uf, SUM(f.valor) as total_despesas,
        COUNT(DISTINCT d.reg_ans) as qtd_operadoras,
        ROUND(SUM(f.valor) / NULLIF(COUNT(DISTINCT d.reg_ans), 0), 2) as media_por_operadora
    FROM fato_despesas f
    JOIN dim_operadoras d ON f.cnpj_origem = d.cnpj
    WHERE f.valor > 0
    GROUP BY d.uf
    ORDER BY total_despesas DESC
    LIMIT 5;
    """
    with db_engine.connect() as conn:
        result = conn.execute(text(sql)).fetchall()
        
        assert len(result) > 0, "Query 2 não retornou resultados!"
        
        print("\nResultado da Query 2: os 5 estados com mais crescimento")
        for row in result:
            print(f"UF: {row[0]} | Total: R$ {row[1]:,.2f} | Média/Op: R$ {row[3]:,.2f}")

def test_query_3_acima_media(db_engine):
    sql = """
    WITH media_mercado_trimestral AS (
        SELECT ano, trimestre, AVG(valor) as media_geral
        FROM fato_despesas WHERE valor > 0 GROUP BY ano, trimestre
    ),
    desempenho_individual AS (
        SELECT f.cnpj_origem, f.ano, f.trimestre, SUM(f.valor) as total_operadora
        FROM fato_despesas f GROUP BY f.cnpj_origem, f.ano, f.trimestre
    )
    SELECT d.razao_social, COUNT(*) as qtd_trimestres_acima_media
    FROM desempenho_individual op
    JOIN media_mercado_trimestral m ON op.ano = m.ano AND op.trimestre = m.trimestre
    JOIN dim_operadoras d ON op.cnpj_origem = d.cnpj
    WHERE op.total_operadora > m.media_geral
    GROUP BY d.razao_social
    HAVING COUNT(*) >= 2
    ORDER BY qtd_trimestres_acima_media DESC
    LIMIT 10;
    """
    with db_engine.connect() as conn:
        result = conn.execute(text(sql)).fetchall()
        
        # O resultado pode ser vazio dependendo dos dados, 
        # mas o teste garante que a sintaxe SQL é válida e não quebra.
        print("\nResultado da Query 3: operadoras acima da média")
        for row in result:
            print(f"Operadora: {row[0]} | Trimestres acima: {row[1]}")