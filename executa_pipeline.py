import subprocess
import sys
import os

SCRIPTS = [
    "etl/download_ans.py",
    "etl/processamento.py",
    "etl/consolidacao.py",
    "etl/validacao.py",
    "etl/enriquecimento.py",
    "etl/agregacao.py",
    "etl/carga_banco.py"
]

def rodar_pipeline():
    print("Iniciando Pipeline de Dados Completo...\n")
    
    if not os.path.exists('etl'):
        print("Erro: Pasta 'etl' não encontrada. Execute este script da raiz do projeto.")
        sys.exit(1)

    for script in SCRIPTS:
        if not os.path.exists(script):
            print(f"Erro: Script não encontrado: {script}")
            sys.exit(1)
            
        print(f"Executando: {script}...")
        
        resultado = subprocess.run([sys.executable, script])
        
        if resultado.returncode != 0:
            print(f"\nFalha na execução do script: {script}")
            print("Pipeline interrompido para correção.")
            sys.exit(resultado.returncode)
            
        print(f"Sucesso: {script}\n" + "-"*40 + "\n")

    print("Pipeline finalizado com sucesso! Todos os processos foram concluídos.")

if __name__ == "__main__":
    rodar_pipeline()
