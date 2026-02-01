# Teste de Desenvolvimento de Software da Intuitive Care

Este repositório contém a solução para o desafio técnico de processamento de dados da ANS (Agência Nacional de Saúde Suplementar). A solução foca em construção de pipelines ETL resilientes, limpeza de dados e análise estruturada.

## Decisões Técnicas da Tarefa 1 - TESTE DE INTEGRAÇÃO COM API PÚBLICA

O padrão ETL foi construido priorizando **flexibilidade**, **simplicidade** e **resiliência**. Abaixo estão os **Trade-off**:

### 1.1. Acesso aos Dados Abertos da ANS via Web Scraping
Utilizei a Abordagem de criar um Crawler Recursivo (`etl/download_ans.py`), devido a estrutura do servidor FTP da ANS variar muito. O script navega dinamicamente por diretórios e subdiretórios para encontrar os arquivos ZIP, evitando hardcoding de URLs. Para obter resiliência às variações, é implementada proteção contra loops de diretórios ("Parent Directory") e verificação de certificados SSL.

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

### Como Executar: 
Pré-requisitos: Python 3.10+

1. Instale as dependências:

    ```pip install -r requirements.txt```

2. Execute o Pipeline ETL (Ordem Sequencial):

    ``` bash
        # Download dos dados
        python -m etl.download_ans

        # Processa e realiza limpeza dos dados
        python -m etl.processamento

        # Processo de consolidação e arquivo zip
        python -m etl.consolidacao
    ```

3. Verifique o Resultado: O arquivo final estará disponível em: ```data/processed/consolidado_despesas.zip```
