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
3. Rode com:
    ```bash
    docker compose up --build
    ```
    Isso irá iniciar os containers do Postgres e MongoDB, além de executar o script
    main.py que vai executar o pipeline ETL.
4. Os dados serão carregados no banco de dados Postgres e replicados no MongoDB.
    Os rsultados estarão disponiveis na pasta `output`.
