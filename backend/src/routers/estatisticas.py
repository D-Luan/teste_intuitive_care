from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func, desc
from .. import database, models, schemas

router = APIRouter(prefix="/api/estatisticas", tags=["Estatísticas"])

@router.get("/", response_model=dict)
def obter_estatisticas(db: Session = Depends(database.get_db)):
    """
    Retorna indicadores consolidados e o ranking das 
    operadoras.
    """
    
    top_5 = db.query(models.EstatisticaAgregada)\
        .order_by(models.EstatisticaAgregada.valor_total.desc())\
        .limit(5)\
        .all()
    
    total_geral = db.query(func.sum(models.EstatisticaAgregada.valor_total)).scalar() or 0
    media_geral = db.query(func.avg(models.EstatisticaAgregada.valor_total)).scalar() or 0

    return {
        "total_geral": float(total_geral),
        "media_geral": float(media_geral),
        "top_5": [
            {"razao_social": t.razao_social, "total": float(t.valor_total)} 
            for t in top_5
        ]
    }