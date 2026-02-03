from sqlalchemy import Column, Integer, String, Numeric
from .database import Base

class Operadora(Base):
    """
    Mapeamento da Tabela Dimensão 
    (SCD Tipo 1 - Atualização de cadastro).
    """
    __tablename__ = "dim_operadoras"

    reg_ans = Column(Integer, primary_key=True, index=True)
    cnpj = Column(String, index=True)
    razao_social = Column(String)
    modalidade = Column(String)
    uf = Column(String)

class Despesa(Base):
    """
    Mapeamento da Tabela Fato.
    """

    __tablename__ = "fato_despesas"

    # No SQLAlchemy, defini a unicidade pela combinação de (CNPJ + Ano + Trimestre),
    # já que o ETL não garante um ID sequencial único para leitura analítica.
    cnpj_origem = Column(String, primary_key=True) 
    ano = Column(Integer, primary_key=True)
    trimestre = Column(Integer, primary_key=True)
    razao_social_snapshot = Column(String)
    valor = Column(Numeric(15, 2))

class EstatisticaAgregada(Base):
    __tablename__ = "agg_despesas"
    
    # Mapeia a coluna no banco de dados 'razaosocial' para o atributo Python 'razao_social'
    razao_social = Column("razaosocial", String, primary_key=True)
    
    uf = Column(String, primary_key=True) 
    valor_total = Column(Numeric(18, 2))
    media_trimestral = Column(Numeric(18, 2))
    desvio_padrao = Column(Numeric(18, 2))