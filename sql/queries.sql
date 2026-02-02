-- TAREFA 3.4 - QUERIES ANALÍTICAS
-- Candidato: Luan Nascimento
-- Banco de Dados: PostgreSQL

-- QUERY 1: Quais as 5 operadoras com maior crescimento PERCENTUAL de despesas?
-- Lógica: (Valor Último Trimestre - Valor Primeiro) / Valor Primeiro
WITH despesas_trimestrais AS (
    SELECT 
        d.razao_social,
        f.ano,
        f.trimestre,
        SUM(f.valor) as total_trimestre
    FROM fato_despesas f
    JOIN dim_operadoras d ON f.cnpj_origem = d.cnpj
    GROUP BY d.razao_social, f.ano, f.trimestre
),
limites_temporais AS (
    -- Uso de Window Functions (FIRST_VALUE) evita Self-Joins custosos
    -- para buscar o primeiro e último registro de cada operadora.
    SELECT DISTINCT
        razao_social,
        FIRST_VALUE(total_trimestre) OVER (
            PARTITION BY razao_social 
            ORDER BY ano ASC, trimestre ASC
        ) as valor_inicial,
        FIRST_VALUE(total_trimestre) OVER (
            PARTITION BY razao_social 
            ORDER BY ano DESC, trimestre DESC
        ) as valor_final
    FROM despesas_trimestrais
)
SELECT 
    razao_social,
    valor_inicial,
    valor_final,
    --- NULLIF evita erro de divisão por zero (ZeroDivisionError)
    ROUND(((valor_final - valor_inicial) / NULLIF(valor_inicial, 0)) * 100, 2) as crescimento_pct
FROM limites_temporais
WHERE valor_inicial > 0
ORDER BY crescimento_pct DESC
LIMIT 5;

-- QUERY 2: Distribuição de despesas por UF (Top 5)
SELECT 
    d.uf,
    SUM(f.valor) as total_despesas,
    COUNT(DISTINCT d.reg_ans) as qtd_operadoras,
    -- Desafio: Média calculada sobre o Distinct de operadoras da UF
    ROUND(SUM(f.valor) / NULLIF(COUNT(DISTINCT d.reg_ans), 0), 2) as media_por_operadora
FROM fato_despesas f
JOIN dim_operadoras d ON f.cnpj_origem = d.cnpj
WHERE f.valor > 0
GROUP BY d.uf
ORDER BY total_despesas DESC
LIMIT 5;

-- QUERY 3: Operadoras acima da média em pelo menos 2 trimestres
-- Trade-off: Uso de CTE (Common Table Expression) vs Subqueries
-- Optei por CTEs para melhorar a legibilidade (Clean Code) e 
-- permitir a reutilização da lógica de "média de mercado" sem duplicar código.
WITH media_mercado_trimestral AS (
    --- Média do mercado por Trimestre
    SELECT 
        ano, 
        trimestre, 
        AVG(valor) as media_geral
    FROM fato_despesas
    WHERE valor > 0
    GROUP BY ano, trimestre
),
desempenho_individual AS (
    -- Total de cada operadora por Trimestre
    SELECT 
        f.cnpj_origem,
        f.ano,
        f.trimestre,
        SUM(f.valor) as total_operadora
    FROM fato_despesas f
    GROUP BY f.cnpj_origem, f.ano, f.trimestre
)
SELECT 
    d.razao_social,
    COUNT(*) as qtd_trimestres_acima_media
FROM desempenho_individual op
JOIN media_mercado_trimestral m 
    ON op.ano = m.ano AND op.trimestre = m.trimestre
JOIN dim_operadoras d 
    ON op.cnpj_origem = d.cnpj
WHERE op.total_operadora > m.media_geral
GROUP BY d.razao_social
HAVING COUNT(*) >= 2
ORDER BY qtd_trimestres_acima_media DESC
LIMIT 10;