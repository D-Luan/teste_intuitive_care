from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List
from .. import database, models, schemas

router = APIRouter(prefix="/api/operadoras", tags=["Operadoras"])

@router.get("/", response_model=schemas.OperadoraPagination)
def listar_operadoras(
    db: Session = Depends(database.get_db),
    page: int = Query(1, ge=1, description="Número da página"),
    limit: int = Query(10, ge=1, le=100, description="Itens por página"),
    search: str = Query(None, description="Busca por Razão Social ou CNPJ")
):
    """
    Lista todas as operadoras cadastradas com suporte a 
    paginação (Offset) e busca textual.
    """
    
    query = db.query(models.Operadora)
    
    if search:
        search_fmt = f"%{search}%"
        query = query.filter(
            (models.Operadora.razao_social.ilike(search_fmt)) | 
            (models.Operadora.cnpj.ilike(search_fmt))
        )
    
    total_registros = query.count()
    
    offset = (page - 1) * limit
    operadoras = query.offset(offset).limit(limit).all()
    
    return {
        "data": operadoras,
        "total": total_registros,
        "page": page,
        "limit": limit
    }

@router.get("/{cnpj}", response_model=schemas.OperadoraDetalhe)
def detalhe_operadora(cnpj: str, db: Session = Depends(database.get_db)):
    """
    Retorna as informações cadastrais detalhadas de uma 
    operadora específica via CNPJ.
    """
    
    op = db.query(models.Operadora).filter(models.Operadora.cnpj == cnpj).first()
    
    if not op:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")
    return op

@router.get("/{cnpj}/despesas", response_model=List[schemas.DespesaBase])
def historico_despesas(cnpj: str, db: Session = Depends(database.get_db)):
    """
    Recupera o histórico cronológico de despesas 
    financeiras de uma operadora.
    """
    
    op = db.query(models.Operadora).filter(models.Operadora.cnpj == cnpj).first()
    
    if not op:
        raise HTTPException(status_code=404, detail="Operadora não encontrada")
    
    despesas = db.query(models.Despesa)\
        .filter(models.Despesa.cnpj_origem == cnpj)\
        .order_by(models.Despesa.ano.desc(), models.Despesa.trimestre.desc())\
        .all()
        
    return despesas