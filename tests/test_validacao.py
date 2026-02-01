import pytest
import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.validacao import validar_cnpj_matematico

def test_cnpjs_validos():
    """
    Verifica se a função aceita CNPJs válidos, com e 
    sem formatação.
    """
    cnpjs_corretos = [
        "00.000.000/0001-91", # Banco do Brasil
        "33.592.510/0001-54", # Vale
        "11222333000181",     # Sem pontuação
        "01.468.594/0001-22"  # Lojas Renner
    ]
    for cnpj in cnpjs_corretos:
        assert validar_cnpj_matematico(cnpj) is True, f"Erro: O CNPJ {cnpj} deveria ser válido!"

def test_cnpjs_invalidos():
    """
    Teste de Stress e Casos Extremos.
    Valida se a função rejeita diferentes tipos de 
    inputs incorretos.
    """

    cnpjs_errados = [
        # Caso 1: Estrutura correta, mas erro matemático no DV
        "00.000.000/0001-90", 
        # Caso 2: Blacklist (Números repetidos passam no cálculo matemático básico, mas são inválidos)
        "11.111.111/1111-11", 
        "00000000000000",   
        # Caso 3: Erros de formato/tamanho 
        "123", 
        # Caso 4: Sanitização (Input sujo ou vazio)              
        "CNPJ_INVALIDO",      
        "",                   
    ]
    for cnpj in cnpjs_errados:
        assert validar_cnpj_matematico(cnpj) is False, f"Erro: O CNPJ {cnpj} deveria ser inválido!"