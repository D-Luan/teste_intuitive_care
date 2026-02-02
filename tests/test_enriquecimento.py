import pytest
import sys
import os
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.enriquecimento import limpar_cnpj_join

def test_cnpj_com_pontuacao():
    """
    Teste de Normalização Padrão.
    Garante que a pontuação não impeça o Join entre 
    bases diferentes.
    """
    entrada = "12.345.678/0001-90"
    esperado = "12345678000190"
    assert limpar_cnpj_join(entrada) == esperado

def test_cnpj_numerico():
    """
    Teste de Robustez de Tipagem.
    Crucial pois o Pandas/Excel frequentemente infere 
    CNPJs como int64, o que quebraria strings operations 
    se não tratado.
    """
    entrada = 12345678000190
    esperado = "12345678000190"
    assert limpar_cnpj_join(entrada) == esperado

def test_cnpj_sujo():
    """
    Teste de Sanitização.
    Simula erros comuns de input manual (espaços extras) 
    que causam falha no 'merge'.
    """
    entrada = " 12.345.678/0001-90  "
    esperado = "12345678000190"
    assert limpar_cnpj_join(entrada) == esperado

def test_valores_nulos():
    """
    Teste de Tratamento de Nulos (NaN/None).
    Evita que o pipeline quebre ao encontrar linhas vazias 
    no CSV da ANS.
    """
    assert limpar_cnpj_join(None) == ""
    assert limpar_cnpj_join(np.nan) == ""
    assert limpar_cnpj_join("") == ""