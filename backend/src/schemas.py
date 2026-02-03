from pydantic import BaseModel
from typing import List, Optional

class OperadoraBase(BaseModel):
    reg_ans: int
    cnpj: str
    razao_social: str
    modalidade: Optional[str] = None
    uf: Optional[str] = None

    class Config:
        from_attributes = True

class OperadoraDetalhe(OperadoraBase):
    pass

class OperadoraPagination(BaseModel):
    data: List[OperadoraBase]
    total: int
    page: int
    limit: int

class DespesaBase(BaseModel):
    ano: int
    trimestre: int
    valor: float

    class Config:
        from_attributes = True

class EstatisticaItem(BaseModel):
    razao_social: str
    valor_total: float