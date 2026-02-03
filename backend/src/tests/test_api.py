from fastapi.testclient import TestClient
from ..main import app

client = TestClient(app)

def test_read_root():
    """
    Verifica se a API está respondendo na raiz.
    """
    
    response = client.get("/")
    assert response.status_code == 200
    assert "message" in response.json()

def test_db_connection():
    """
    Valida a conexão real entre API e PostgreSQL.
    """
    
    response = client.get("/health/db")
    assert response.status_code == 200
    assert response.json() == {"status": "ok", "database": "Conectado"}

def test_listar_operadoras_paginacao():
    """Valida se a paginação respeita os limites e o 
    formato de resposta do pacote."""
    
    response = client.get("/api/operadoras/?page=1&limit=5")
    assert response.status_code == 200
    
    data = response.json()
    
    assert "data" in data
    assert "total" in data
    assert "page" in data
    assert data["page"] == 1
    assert data["limit"] == 5
    assert len(data["data"]) == 5

def test_buscar_operadora_especifica():
    # Usei um CNPJ conhecido (2CARE) reconhecido no CSV de origem.
    # Se a base de dados mudar, este CNPJ precisa ser atualizado.
    
    cnpj_teste = "27452545000195"
    response = client.get(f"/api/operadoras/{cnpj_teste}")
    
    assert response.status_code == 200
    data = response.json()
    
    assert data["cnpj"] == cnpj_teste
    assert "razao_social" in data
    assert "2CARE" in data["razao_social"]

def test_buscar_operadora_inexistente():
    response = client.get("/api/operadoras/99999999000199")
    assert response.status_code == 404

def test_estatisticas_gerais():
    """Valida o processamento de agregação e o 
    ranking das top 5 operadoras."""
    
    response = client.get("/api/estatisticas/")
    assert response.status_code == 200
    
    data = response.json()

    assert "total_geral" in data
    assert "media_geral" in data
    assert "top_5" in data

    assert isinstance(data["total_geral"], (int, float))
    assert isinstance(data["media_geral"], (int, float))
    assert isinstance(data["top_5"], list)
    assert len(data["top_5"]) <= 5

    if data["top_5"]:
        for item in data["top_5"]:
            assert "razao_social" in item
            assert "total" in item
            assert isinstance(item["total"], (int, float))

def test_historico_despesas_operadora():
    """Verifica se o histórico de despesas retorna uma 
    lista e possui a estrutura correta."""
    
    cnpj_teste = "27452545000195" 
    response = client.get(f"/api/operadoras/{cnpj_teste}/despesas")
    
    assert response.status_code == 200
    data = response.json()
    
    assert isinstance(data, list)
    
    if len(data) > 0:
        item = data[0]
        assert "ano" in item
        assert "trimestre" in item
        assert "valor" in item