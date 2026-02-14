import subprocess
import sys
import os

def executar_testes():
    """
    Script orquestrador para rodar os testes automatizados.
    Cobre: Agregação, Banco, Enriquecimento, Queries e Validação.
    """
    print("Iniciando Execução de Testes do Pipeline ETL...")
    print("=" * 60)

    pasta_testes = "tests"
    if not os.path.exists(pasta_testes):
        print(f"Erro: A pasta '{pasta_testes}' não foi encontrada na raiz.")
        sys.exit(1)

    comando = [
        sys.executable, "-m", "pytest", 
        pasta_testes, 
        "-v", 
        "--disable-warnings"
    ]

    try:
        resultado = subprocess.run(comando)

        print("=" * 60)
        if resultado.returncode == 0:
            print("SUCESSO: Todos os testes passaram!")
        else:
            print("FALHA: Alguns testes não passaram. Verifique o log.")
            sys.exit(resultado.returncode)

    except FileNotFoundError:
        print("Erro: O 'pytest' não está instalado.")
        sys.exit(1)
    except Exception as e:
        print(f"Erro inesperado: {e}")
        sys.exit(1)

if __name__ == "__main__":
    executar_testes()