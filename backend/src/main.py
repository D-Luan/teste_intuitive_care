from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from . import database, models

app = FastAPI(
    title="API de Despesas ANS",
    description="API para consulta de dados financeiros de operadoras de saúde.",
    version="1.0.0"
)

@app.get("/")
def read_root():
    return {"message": "API do Teste Intuitive Care está online!"}

@app.get("/health/db")
def test_db_connection(db: Session = Depends(database.get_db)):
    """
    Health Check:
    Executa uma query leve para garantir que a aplicação
    possui conectividade ativa com o PostgreSQL.
    """
    
    try:
        result = db.execute(text("SELECT 1")).scalar()
        if result == 1:
            return {"status": "ok", "database": "Conectado"}
        else:
            raise HTTPException(status_code=500, detail="Retorno inesperado do banco.")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro de conexão: {str(e)}")