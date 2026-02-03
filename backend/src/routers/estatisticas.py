from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from .. import database, models

router = APIRouter(prefix="/api/estatisticas", tags=["Estatísticas"])

@router.get("/", response_model=dict)
def obter_estatisticas(db: Session = Depends(database.get_db)):
    """
    Retorna indicadores consolidados, ranking e 
    distribuição geográfica para gráficos.
    """
    
    top_5 = db.query(models.EstatisticaAgregada)\
        .order_by(models.EstatisticaAgregada.valor_total.desc())\
        .limit(5)\
        .all()
    
    total_geral = db.query(func.sum(models.EstatisticaAgregada.valor_total)).scalar() or 0
    media_geral = db.query(func.avg(models.EstatisticaAgregada.valor_total)).scalar() or 0

    # Consolida os valores por Estado para alimentar gráficos de mapa/barras no Frontend.
    por_uf = db.query(
        models.EstatisticaAgregada.uf,
        func.sum(models.EstatisticaAgregada.valor_total).label("total")
    ).group_by(models.EstatisticaAgregada.uf)\
     .order_by(func.sum(models.EstatisticaAgregada.valor_total).desc())\
     .all()

    return {
        "total_geral": float(total_geral),
        "media_geral": float(media_geral),
        "top_5": [
            {"razao_social": t.razao_social, "total": float(t.valor_total)} 
            for t in top_5
        ],
        "por_uf": [
            {"uf": row.uf, "total": float(row.total)}
            for row in por_uf
        ]
    }