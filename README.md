# Desafio técnico - Dev Python Pl

Este projeto realiza um pipeline ETL com dados de vendas, clientes e produtos,
gerando indicadores e exportando resultados.

## Requisitos
- Docker compose
- Linux ou WSL (Não tenho certeza se funciona no Windows)

## Running

1. Clone o repositório
2. Os dados compactados devem estar na pasta `data`:
    - `vendas.zip`
    - `clientes.zip`
    - `produtos.zip`
3. Antes de iniciar, crie um arquivo `.env` na raiz do projeto e preencha com as variáveis de ambiente necessárias. Exemplo de conteúdo:

    ```
    POSTGRES_HOST=postgres
    POSTGRES_PORT=5432
    POSTGRES_USER=etluser
    POSTGRES_PASSWORD=etlpass
    POSTGRES_DB=etldb
    MONGO_HOST=mongodb
    MONGO_PORT=27017
    MONGO_USER=mongouser
    MONGO_PASSWORD=mongopass
    MONGO_DB=etldb
    ```
4. Rode com:
    ```bash
    docker compose up --build
    ```
    Isso irá iniciar os containers do Postgres e MongoDB, além de executar o script
    main.py que vai executar o pipeline ETL.
5. Os dados serão carregados no banco de dados Postgres e replicados no MongoDB.
    Os rsultados estarão disponiveis na pasta `output`.

## Diagrama geral de funcionamento

```
      +------------------+
      |  Arquivos .zip   |
      | (vendas,         |
      |  clientes,       |
      |  produtos)       |
      +--------+---------+
               |
               v
      +--------+---------+
      |  Extração &      |
      |  Normalização    |
      |  (pandas)        |
      +--------+---------+
               |
               v
      +--------+---------+
      |  Validação &     |
      |  Enriquecimento  |
      |  (tratamento de  |
      |  dados,          |
      |  consistência,   |
      |  join, etc)      |
      +--------+---------+
               |
               v
      +--------+---------+
      | Carga Relacional |
      | (PostgreSQL)     |
      +----+------+------+
           |      |
           |      +-------------------------------+
           |                                      |
           v                                      v
  +--------+---------+                  +---------+---------+
  | Exportação SQL   |                  | Replicação MongoDB|
  | (indicadores     |                  | (clientes)        |
  |  vendas, clientes|                  +-------------------+
  |  produtos, etc.) |
  +--------+---------+
           |
           v
  +--------+---------+
  |  Geração de      |
  |  Arquivos CSV    |
  |  e Parquet       |
  |  (outputs/)      |
  +------------------+
```
