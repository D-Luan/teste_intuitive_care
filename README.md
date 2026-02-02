# Teste de Desenvolvimento de Software da Intuitive Care

Este repositório contém a solução para o desafio técnico de processamento de dados da ANS (Agência Nacional de Saúde Suplementar). A solução foca em construção de pipelines ETL resilientes, limpeza de dados e análise estruturada.

## Decisões Técnicas da Tarefa 1 - TESTE DE INTEGRAÇÃO COM API PÚBLICA

O padrão ETL foi construido priorizando **flexibilidade**, **simplicidade** e **resiliência**. Abaixo estão os **Trade-off**:

### 1.1. Acesso aos Dados Abertos da ANS via Web Scraping
Utilizei a Abordagem de criar um Crawler Recursivo (`etl/download_ans.py`), devido a estrutura do servidor FTP da ANS variar muito. O script navega dinamicamente por diretórios e subdiretórios para encontrar os arquivos ZIP, evitando hardcoding de URLs. Para obter resiliência às variações, é implementada proteção contra loops de diretórios ("Parent Directory") e tratamento de exceções de SSL.

Exemplo da Lógica central de navegação e extração:

```python
for link in soup.find_all('a', href=True):
    href = link['href']
    url_completa = urljoin(url_atual, href)

    # Se for um arquivo zip e conter ano (ex: 2023), adiciona à lista
    if '.zip' in href.lower() and re.search(r'202\d', href):
        zips_encontrados.append(url_completa)
    
    # Se a pasta for válida, executa a recursão (DFS)
    elif href.endswith('/') and href not in ['../', './']:
        sub_zips = buscar_zips_recursivamente(url_completa, nivel + 1)
        zips_encontrados.extend(sub_zips)
```

### 1.2. Processamento de Arquivos
Optei pelo **Processamento Incremental**. Dessa forma, o script `etl/processamento.py` processa um arquivo trimestral por vez (`Open -> Transform -> Append -> Close`). Isso garante que o pipeline rode com consumo de RAM baixo e constante (O(1)), independentemente do volume histórico de dados, evitando estouro de memória (OOM) em ambientes conteinerizados.

Exemplo da pipeline de leitura e escrita incremental (Append Pattern):

```python
with zipfile.ZipFile(caminho_zip, 'r') as z:
    # Carrega apenas o arquivo atual na memória
    with z.open(nome_arquivo_interno) as f:
        df = carregar_dataframe_robusto(f, ...)
        
        # ... Aplicação de filtros e regras de negócio ...

        # Despeja no disco imediatamente (mode='a') e libera a RAM
        modo = 'w' if primeiro_arquivo else 'a'
        df_final.to_csv(ARQUIVO_SAIDA, mode=modo, header=primeiro_arquivo, index=False)
```

### 1.3. Consolidação e Tratamento de Inconsistências
Como os CSVs brutos não possuem coluna de data confiável, o `Trimestre` e `Ano` são extraídos via **Regex** diretamente do nome do arquivo original (`1T2025.zip`). No cruzamento com o CADOP, optei por um **Left Join** (priorizando as Despesas). Dessa forma, manter a integridade financeira é prioritário. Se uma operadora tem despesas mas não está no cadastro, ela aparece como "DESCONHECIDA", mas o valor contábil é preservado. Por fim, o arquivo CADOP passa por uma deduplicação prévia baseada no `Registro_ANS` para evitar a multiplicação de linhas no relatório final.

Exemplo da estratégia de Join visando integridade contábil:
```python
# Deduplicação preventiva da tabela dimensional (Cadop)
df_cadop.drop_duplicates(subset=['reg_ans'], inplace=True)

# Cruzamento (Left Join), prioriza a tabela da esquerda (Despesas). Se não achar a operadora, mantém o dado.
df_final = pd.merge(df_despesas, df_cadop, on='reg_ans', how='left')

# Tratamento de Missing Data
df_final['RazaoSocial'] = df_final['RazaoSocial'].fillna("DESCONHECIDA")
```

## Decisões Técnicas da Tarefa 2 - TESTE DE TRANSFORMAÇÃO E VALIDAÇÃO DE DADOS

Esta etapa focou na garantia de qualidade (Data Quality), enriquecimento e agregação estatística.

### 2.1. Estratégia de Validação (Data Quality)
Encontrei inconsistências como CNPJs com dígitos verificadores inválidos e valores negativos (estornos contábeis). Minha decisão foi usar  **Flagging**. Em vez de excluir registros inválidos (o que geraria perda de integridade financeira no balanço), optei por criar uma coluna `status_auditoria`. Registros inválidos são mantidos no cálculo financeiro, mas sinalizados para revisão posterior.

Exemplo de uso do Flagging
```python
df['check_cnpj'] = df['CNPJ'].apply(validar_cnpj_matematico)
df['check_valor'] = df['ValorDespesas'] > 0

# Estratégia de "Soft Validation" (Flagging)
# Em vez de excluir linhas (df.drop), consolida os erros em metadados.
def definir_status(row):
    erros = []
    if not row['check_cnpj']: erros.append("CNPJ_INVALIDO")
    if not row['check_valor']: erros.append("VALOR_INVALIDO")
    
    return "OK" if not erros else "; ".join(erros)

df['status_auditoria'] = df.apply(definir_status, axis=1)
```

### 2.2. Enriquecimento e Join
Foi necessário cruzar as despesas com o cadastro de operadoras usando o **CNPJ** como chave. Como uma forma mais simplificada e rápida para o Join, decidi usar o **In-Memory Hash Join (Pandas Merge)**. Dado o volume final (~80k linhas na tabela fato e ~1k na dimensão), o uso de engines distribuídas (Spark) seria exagerado e provavelmente me custaria tempo. O Pandas gerencia esse volume em milissegundos com baixo custo de memória. Para o tratamento de falhas, o arquivo Cadop foi deduplicado mantendo a primeira ocorrência (`keep='first'`) para evitar o erro de "Fan-out" (multiplicação de linhas no join) durante o cruzamento. E para o LEFT JOIN, priorizei a tabela de Despesas. Operadoras sem cadastro ativo no Cadop não são descartadas, mas sim preenchidas como "DESCONHECIDA" para preservar o valor contábil total.

```python
df_despesas['CNPJ_KEY'] = df_despesas['CNPJ'].apply(limpar_cnpj_join)
df_cadop['CNPJ_KEY'] = df_cadop['CNPJ'].apply(limpar_cnpj_join)

# Remove duplicatas mantendo a primeira ocorrência para impedir 
# a multiplicação de linhas (Fan-out) no Join.
df_cadop = df_cadop.drop_duplicates(subset=['CNPJ_KEY'], keep='first')

df_final = pd.merge(df_despesas, cols_cadop, on='CNPJ_KEY', how='left')

# Tratamento de 'Sem Match' (Dados Faltantes)
cols_novas = ['RegistroANS_Novo', 'Modalidade', 'UF']
for col in cols_novas:
    df_final[col] = df_final[col].fillna("DESCONHECIDO")
```

### 2.3. Agregação e Estatística
O cálculo de média seguiu a regra de negócio de **dois níveis**: Soma das despesas **por trimestre** (visão temporal) e Média e Desvio Padrão calculados sobre os **totais trimestrais** (visão por entidade).
Para a ordenação, eu escolhi o algoritmo **QuickSort** (nativo via `sort_values`). Após a agregação, o dataset é reduzido para nível de operadora (< 1.000 linhas). Ordenação em memória é a abordagem mais eficiente (Complexidade O(N log N)) para este volume.

```python
df_trimestral = df.groupby(['RazaoSocial', 'UF', 'Ano', 'Trimestre'])['ValorDespesas'].sum().reset_index()
df_trimestral.rename(columns={'ValorDespesas': 'TotalTrimestre'}, inplace=True)

# Calcula Média e Desvio Padrão baseados nos totais trimestrais.
df_agregado = df_trimestral.groupby(['RazaoSocial', 'UF']).agg(
    Valor_Total=('TotalTrimestre', 'sum'),
    Media_Trimestral=('TotalTrimestre', 'mean'),
    Desvio_Padrao=('TotalTrimestre', 'std')
).reset_index()

# Tratamento de NaN para operadoras com apenas um trimestre
df_agregado['Desvio_Padrao'] = df_agregado['Desvio_Padrao'].fillna(0.0)

# Ordenação (In-Memory QuickSort)
df_agregado.sort_values(by='Valor_Total', ascending=False, inplace=True)
```

## Decisões Técnicas da Tarefa 3 - TESTE DE BANCO DE DADOS E ANÁLISE

Esta etapa consistiu na modelagem, carga e análise de dados utilizando **PostgreSQL** (via Docker).

### 3.1 e 3.3. Estratégia de Carga e Tratamento de Inconsistências
Devido à arquitetura containerizada (Docker), a carga de dados via `LOAD DATA INFILE` nativo apresentaria complexidade de permissões de volume. Como abordagem, usei o Script Python (`etl/carga_banco.py`) utilizando `SQLAlchemy` e `Pandas`. O qual permite tratamento prévio de inconsistências (limpeza de `NULLs`, conversão de encoding `ISO-8859-1` para `UTF-8`) antes da inserção, garantindo que apenas dados sanitizados entrem no banco.

Exemplo para a sanitização antes da carga do banco de dados
```python
df['valor'] = pd.to_numeric(df['valor'], errors='coerce').fillna(0)

# Mapea explicitamente para Numeric(15, 2) no banco.
# O uso de FLOAT é evitado para garantir precisão financeira.
df.to_sql('fato_despesas', engine, if_exists='replace', index=False,
          dtype={'valor': Numeric(15, 2), 'ano': Integer(), 'trimestre': Integer()})

# Como o Pandas não cria índices nativamente, executei DDL 
# manual para otimizar as queries analíticas subsequentes.
with engine.connect() as conn:
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fato_cnpj ON fato_despesas(cnpj_origem);"))
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fato_tempo ON fato_despesas(ano, trimestre);"))
    conn.commit()
```

### 3.2. Modelagem de Dados e Trade-offs
Para atender aos requisitos de performance e integridade, tomei as seguintes decisões:

#### **A. Normalização: Opção Tabelas normalizadas separadas**
O modelo foi separado em **Fato** (`fato_despesas`) e **Dimensão** (`dim_operadoras`).
Os Dados cadastrais são mutáveis. A normalização garante que uma alteração cadastral seja feita em um único registro, refletindo automaticamente em todas as transações. A tabela fato possui ~76 mil registros. Repetir textos longos em cada linha aumentaria desnecessariamente o I/O de disco e o uso de memória.

#### **B. Tipos de Dados**
Para valores monetários, optei por **DECIMAL** em vez de `FLOAT` porque tipos de ponto flutuante (`FLOAT`) introduzem erros de arredondamento (ex: `0.1 + 0.2 != 0.3`) inaceitáveis para dados financeiros.
Para datas, colunas `ano` e `trimestre` foram tipadas como Inteiros porque a granularidade da análise é trimestral. Usar `DATE` exigiria funções de extração (`EXTRACT(YEAR FROM...)`) em todas as queries, reduzindo a performance de índices sem ganho funcional.

#### **C. Performance e Índices**
Para garantir a velocidade das queries analíticas, foram criados índices B-Tree explícitos:
* `idx_fato_cnpj`: Acelera o JOIN entre a Fato e a Dimensão.
* `idx_fato_tempo`: Acelera filtros temporais (`WHERE ano = X`).
* `PK (reg_ans)`: Garante unicidade e integridade referencial na dimensão.

Exemplo para a modelagem no banco de dados
```python
# Numeric(15,2) é essencial para evitar erros de arredondamento em dados financeiros.
# Integer é melhor performance de indexação que Date para esta granularidade.
type_fato = {
    'valor': Numeric(15, 2),
    'ano': Integer(),     
    'trimestre': Integer(),
    'cnpj_origem': String(20)
}

df.to_sql('fato_despesas', engine, if_exists='replace', index=False, dtype=dtype_fato)

# O Pandas não gerencia chaves ou índices. Executei DDL manual para:
# Definir Primary Key na dimensão.
# Criar índices B-Tree para acelerar Joins e Filtros Temporais.
with engine.connect() as conn:
    conn.execute(text("ALTER TABLE dim_operadoras ADD PRIMARY KEY (reg_ans);"))

    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fato_cnpj ON fato_despesas(cnpj_origem);"))
    
    conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fato_tempo ON fato_despesas(ano, trimestre);"))
```
### 3.4. Queries Analíticas (`sql/queries.sql`)
As queries foram desenvolvidas focando em legibilidade e performance.

Para o crescimento trimestral, utilizei Window Functions (FIRST_VALUE) com ordenações opostas (ASC/DESC). Isso permite capturar o primeiro e o último registro de cada operadora em uma única passagem (Scan), eliminando a necessidade de múltiplos self-joins custosos. Para a distribuição geográfica, usei agregação simples com tratamento de divisão por zero (NULLIF). Por fim, optei por CTEs (Common Table Expressions) para isolar o cálculo da média de mercado, tornando o código mais legível e modular comparado a subqueries aninhadas.

```sql
WITH limites_temporais AS (
    -- Uso de Window Functions evita Self-Joins.
    -- Busca o primeiro e último valor manipulando apenas a ordem (ASC/DESC) na partição.
    SELECT DISTINCT
        razao_social,
        FIRST_VALUE(total_trimestre) OVER (PARTITION BY ... ORDER BY ... ASC) as valor_inicial,
        FIRST_VALUE(total_trimestre) OVER (PARTITION BY ... ORDER BY ... DESC) as valor_final
    FROM despesas_trimestrais
)
SELECT 
    razao_social,
    -- NULLIF trata operadoras que iniciaram zeradas, evitando erro de "Division by Zero" que derrubaria a 
    --- execução do relatório.
    ROUND(((valor_final - valor_inicial) / NULLIF(valor_inicial, 0)) * 100, 2) as crescimento_pct
FROM limites_temporais
WHERE valor_inicial > 0
ORDER BY crescimento_pct DESC
LIMIT 5;
```

### Como Executar: 
Pré-requisitos: 
- Python 3.10+
- PostgreSQL (Local ou Docker)

1. Configuração de Ambiente Crie um arquivo .env na raiz do projeto com as credenciais do banco (ou use o padrão do código):
```bash
DB_HOST=localhost
DB_NAME=postgres
DB_USER=postgres
DB_PASSWORD=postgres
```
(Opcional) Se tiver Docker instalado, suba um banco rapidamente:

```bash
docker run --name pg-test -e POSTGRES_PASSWORD=postgres -p 5432:5432 -d postgres:13
```

1. Instale as dependências:

    ```pip install -r requirements.txt```

2. Execute os Testes Unitários (Opcional):

    ```pytest```

2. Execute o Pipeline ETL:

    Tarefa 1: ETL e Consolidação
    ``` bash
        # Download dos dados
        python -m etl.download_ans

        # Processa e realiza limpeza dos dados
        python -m etl.processamento

        # Processo de consolidação e arquivo zip
        python -m etl.consolidacao
    ```

    Tarefa 2: Qualidade, Enriquecimento e Agregação
    ``` bash
        # Validação de Dados
        python -m etl.validacao

        # Enriquecimento
        python -m etl.enriquecimento

        # Agregação e Estatística
        python -m etl.agregacao
    ```

    Tarefa 3: Banco de Dados e SQL
    ``` bash
        # Carga Otimizada no Banco
        python -m etl.carga_banco
    ```

    Execução de Testes:
    ``` bash
        # Testes Unitários (Lógica de ETL)
        pytest tests/test_agregacao.py tests/test_validacao.py tests/test_enriquecimento.py

        # Testes de Integração (Banco de Dados e Queries SQL)
        pytest tests/test_banco.py tests/test_queries.py
    ```

3. Verifique os arquivos: Os arquivos solicitados no teste estarão disponíveis em:

    - **ETL:** ```data/processed/consolidado_despesas.zip```
    - **Agregação:** ```data/processed/teste_seu_nome.zip```
