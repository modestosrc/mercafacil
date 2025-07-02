import os
import gc
from etl import ingest, transform, load, export, sql_indicators
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

# TODO:
# - Revisar os tipos
DATA = {
    "clientes": {
        "path": "data/clientes.zip",
        "types": {
            "COD_ID_CLIENTE": "int32",
            "DES_TIPO_CLIENTE": "category",
            "NOM_NOME": "string",
            "DES_SEXO_CLIENTE": "category",
            "DAT_DATA_NASCIMENTO": "string"
        }
    },
    "vendas": {
        "path": "data/vendas.zip",
        "types": {
            "COD_ID_LOJA": "int32",
            "NUM_ANOMESDIA": "int32",
            "COD_ID_CLIENTE": "int32",
            "DES_TIPO_CLIENTE": "category",
            "DES_SEXO_CLIENTE": "category",
            "COD_ID_VENDA_UNICO": "string",
            "COD_ID_PRODUTO": "int32",
            "VAL_VALOR_SEM_DESC": "float32",
            "VAL_VALOR_DESCONTO": "float32",
            "VAL_VALOR_COM_DESC": "float32",
            "VAL_QUANTIDADE_KG": "float32"
        }
    },
    "produtos": {
        "path": "data/produtos.zip",
        "types": {
            "COD_ID_PRODUTO": "int32",
            "COD_ID_CATEGORIA_PRODUTO": "int32",
            "ARR_CATEGORIAS_PRODUTO": "string",
            "DES_PRODUTO": "string",
            "DES_UNIDADE": "category",
            "COD_CODIGO_BARRAS": "string"
        }
    }
}


def main():
    # 1. Ingestão dos dados
    dataframes = ingest.ingest(DATA)
    gc.collect()
    logging.info("[LOG] Ingestão concluída com sucesso.")

    # 2. Transformação e enriquecimento
    vendas, divergencias = transform.transform(dataframes)
    del dataframes
    gc.collect()
    logging.info(
        "[LOG] Transformação e enriquecimento concluídos com sucesso.")

    # 3. Load: salva no Postgres e replica clientes no Mongo
    POSTGRES_CONN = (
        f"""postgresql://{os.getenv('POSTGRES_USER')
                          }:{os.getenv('POSTGRES_PASSWORD')}"""
        f"""@{os.getenv('POSTGRES_HOST')}:{os.getenv('POSTGRES_PORT')
                                           }/{os.getenv('POSTGRES_DB')}"""
    )
    load.load_to_postgres(vendas, "vendas", POSTGRES_CONN)
    logging.info(
        "[LOG] Vendas carregadas no PostgreSQL com sucesso.")
    load.replicate_clientes_to_mongo(
        vendas[["COD_ID_CLIENTE", "NOM_NOME"]].drop_duplicates(),
        f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}"
        f"@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}/",
        os.getenv('MONGO_DB'),
        "clientes"
    )
    gc.collect()
    logging.info(
        "[LOG] Clientes replicados no MongoDB com sucesso.")

    # 4. Exporta vendas enriquecidas em parquet particionado por ano/mês
    export.export_vendas_parquet(vendas, "outputs/vendas_parquet/")
    del vendas
    gc.collect()
    logging.info(
        "[LOG] Vendas exportadas em parquet particionado com sucesso.")

    # 5. Exporta divergências para CSV
    export.export_divergencias_csv(divergencias, "outputs/divergencias/")
    logging.info(
        "[LOG] Divergências exportadas para CSV com sucesso.")

    # 6. Indicadores via SQL
    logging.info("[LOG] Gerando indicadores via SQL...")
    os.makedirs("outputs/indicadores_sql", exist_ok=True)
    sql_indicators.export_produtos_mais_vendidos(
        POSTGRES_CONN, "outputs/indicadores_sql/produtos_mais_vendidos.csv")
    sql_indicators.export_clientes_mais_compraram(
        POSTGRES_CONN, "outputs/indicadores_sql/clientes_mais_compraram.csv")
    sql_indicators.export_vendas_por_dia(
        POSTGRES_CONN, "outputs/indicadores_sql/vendas_por_dia.csv")
    sql_indicators.export_produtos_distintos_por_dia(
        POSTGRES_CONN, "outputs/indicadores_sql/produtos_distintos_por_dia.csv")
    sql_indicators.export_produtos_maior_desconto(
        POSTGRES_CONN, "outputs/indicadores_sql/produtos_maior_desconto.csv")
    sql_indicators.export_melhor_dia_semana_top_clientes(
        POSTGRES_CONN, "outputs/indicadores_sql/top20_melhor_dia_semana.csv")
    logging.info("[LOG] Indicadores SQL exportados com sucesso.")


if __name__ == "__main__":
    logging.info("[LOG] Iniciando o processo ETL...")
    main()
