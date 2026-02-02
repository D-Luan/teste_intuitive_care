import pytest
import sys
import os
import pandas as pd
import numpy as np

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.agregacao import calcular_estatisticas_operadora

def test_agregacao_logica_estatistica():
    """
    Teste de Regra de Negócio:
    Verifica se a média é calculada sobre o TOTAL do 
    trimestre, e não sobre a média simples dos lançamentos 
    contábeis (o que seria um erro analítico).
    """
    
    # Contextos dos cenários:
    # Trimestre 1: 50.0 + 50.0 = 100.0
    # Trimestre 2: 300.0
    # Média Esperada (Correta): (100 + 300) / 2 = 200.0
    # Média Errada (Simples): (50 + 50 + 300) / 3 = 133.33
    dados = {
        'RazaoSocial': ['Op X', 'Op X', 'Op X'],
        'UF': ['SP', 'SP', 'SP'],
        'Ano': ['2023', '2023', '2023'],
        'Trimestre': ['1', '1', '2'],
        'ValorDespesas': [50.0, 50.0, 300.0]
    }
    df_mock = pd.DataFrame(dados)

    resultado = calcular_estatisticas_operadora(df_mock)
    linha_resultado = resultado.iloc[0]

    assert linha_resultado['Valor_Total'] == 400.0
    
    assert linha_resultado['Media_Trimestral'] == 200.0, "Erro: A média deve ser por trimestre, não por linha!"

    assert pytest.approx(linha_resultado['Desvio_Padrao'], 0.01) == 141.42

def test_agregacao_zero_nans():
    """
    Teste de Robustez:
    Garante que operadoras sem valores ou com dados 
    corrompidos (None/NaN) sejam tratadas como 0.0, 
    evitando quebras no cálculo estatístico (divisão 
    por zero/NaN).
    """
    
    dados = {
        'RazaoSocial': ['Op Y'],
        'UF': ['RJ'],
        'Ano': ['2023'],
        'Trimestre': ['1'],
        'ValorDespesas': [None]
    }
    df_mock = pd.DataFrame(dados)
    
    resultado = calcular_estatisticas_operadora(df_mock)
    
    assert resultado.iloc[0]['Valor_Total'] == 0.0
    assert resultado.iloc[0]['Desvio_Padrao'] == 0.0