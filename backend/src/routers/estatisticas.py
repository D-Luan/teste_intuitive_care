from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from .. import database

router = APIRouter(prefix="/api/estatisticas", tags=["Estatísticas"])

@router.get("/", response_model=dict)
def obter_estatisticas(db: Session = Depends(database.get_db)):
    """
    Retorna indicadores consolidados, ranking e 
    distribuição geográfica para gráficos.
    """

    # Agrupamos por Razão Social e UF para reduzir a granularidade dos dados
    # antes de trazer para a aplicação.
    sql = text("""
        SELECT 
            op.razao_social, 
            op.uf, 
            SUM(f.valor) as total
        FROM fato_despesas f
        JOIN dim_operadoras op ON f.cnpj_origem = op.cnpj
        GROUP BY op.razao_social, op.uf
        ORDER BY total DESC
    """)
    
    results = db.execute(sql).fetchall()
    
    dados_processados = []
    total_geral = 0.0
    
    for row in results:
        val = float(row.total)
        dados_processados.append({
            "razao_social": row.razao_social,
            "uf": row.uf,
            "total": val
        })
        total_geral += val

    qtd = len(dados_processados)
    media = total_geral / qtd if qtd > 0 else 0
    
    top_5 = dados_processados[:5]

    uf_dict = {}
    for item in dados_processados:
        uf = item['uf']
        if uf not in uf_dict:
            uf_dict[uf] = 0.0
        uf_dict[uf] += item['total']
    
    por_uf = [{"uf": k, "total": v} for k, v in uf_dict.items()]
    por_uf.sort(key=lambda x: x['total'], reverse=True)

    return {
        "total_geral": total_geral,
        "media_geral": media,
        "top_5": top_5,
        "por_uf": por_uf
    }