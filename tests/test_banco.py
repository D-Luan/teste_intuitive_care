import pytest
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DB_USER = os.getenv('DB_USER', 'postgres')
DB_PASS = os.getenv('DB_PASSWORD', 'postgres')
DB_NAME = os.getenv('DB_NAME', 'postgres')
DB_HOST = "localhost"
DB_PORT = "5432"
DB_CONNECTION_STR = f'postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

@pytest.fixture(scope="module")
def db_engine():
    """
    Fixture que mantém a conexão viva durante todo o 
    módulo de testes.
    """

    engine = create_engine(DB_CONNECTION_STR)
    yield engine
    engine.dispose()

def test_conexao_banco(db_engine):
    try:
        with db_engine.connect() as conn:
            result = conn.execute(text("SELECT 1")).scalar()
            assert result == 1
    except Exception as e:
        pytest.fail(f"Falha de conexão: {e}")

def test_volume_dados(db_engine):
    """
    Teste de Integridade do ETL:
    Verifica se as tabelas essenciais foram criadas e se 
    não estão vazias (garante que a carga de dados ocorreu).
    """
    
    tabelas = ['dim_operadoras', 'fato_despesas', 'agg_despesas']
    
    with db_engine.connect() as conn:
        for tab in tabelas:
            exists = conn.execute(text(f"SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = '{tab}')")).scalar()
            assert exists is True, f"Tabela '{tab}' não foi criada!"
            
            qtd = conn.execute(text(f"SELECT COUNT(*) FROM {tab}")).scalar()
            assert qtd > 0, f"Tabela '{tab}' está vazia (0 registros)!"

def test_primary_key_existe(db_engine):
    """
    Validação de Schema (Constraint):
    Como o Pandas 'to_sql' não cria PKs nativamente, este teste garante 
    que o comando ALTER TABLE manual foi executado com sucesso na dimensão.
    """
    
    sql_check_pk = """
        SELECT constraint_name 
        FROM information_schema.table_constraints 
        WHERE table_name = 'dim_operadoras' AND constraint_type = 'PRIMARY KEY';
    """
    with db_engine.connect() as conn:
        pk = conn.execute(text(sql_check_pk)).scalar()
        assert pk is not None, "ERRO: Primary Key não encontrada na tabela 'dim_operadoras'!"

def test_indices_performance_existem(db_engine):
    """
    Verificação de Performance:
    Confirma se os índices estratégicos realmente existem 
    na estrutura física do banco.
    """
    
    sql_check_indexes = """
        SELECT indexname 
        FROM pg_indexes 
        WHERE tablename = 'fato_despesas';
    """
    with db_engine.connect() as conn:
        indexes = [row[0] for row in conn.execute(text(sql_check_indexes)).fetchall()]
        
        assert 'idx_fato_cnpj' in indexes, "ERRO: Índice de CNPJ (idx_fato_cnpj) faltando na fato!"
        assert 'idx_fato_tempo' in indexes, "ERRO: Índice Temporal (idx_fato_tempo) faltando na fato!"