import pandas as pd
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def run_query_to_csv(query, conn_string, out_path):
    engine = create_engine(conn_string)
    with engine.connect() as conn:
        df = pd.read_sql_query(query, conn)
        df.to_csv(out_path, index=False)
    logging.info(f"Indicador exportado para {out_path}")


def export_produtos_mais_vendidos(conn_string, out_path):
    query = """
        SELECT "COD_ID_PRODUTO", COUNT(*) AS qtd_vendas
        FROM vendas
        GROUP BY "COD_ID_PRODUTO"
        ORDER BY qtd_vendas DESC
    """
    run_query_to_csv(query, conn_string, out_path)


def export_clientes_mais_compraram(conn_string, out_path):
    query = """
        SELECT "COD_ID_CLIENTE", COUNT(*) AS qtd_compras
        FROM vendas
        GROUP BY "COD_ID_CLIENTE"
        ORDER BY qtd_compras DESC
    """
    run_query_to_csv(query, conn_string, out_path)


def export_vendas_por_dia(conn_string, out_path):
    query = """
        SELECT "NUM_ANOMESDIA", COUNT(DISTINCT "COD_ID_VENDA_UNICO") AS qtd_vendas
        FROM vendas
        GROUP BY "NUM_ANOMESDIA"
        ORDER BY "NUM_ANOMESDIA"
    """
    run_query_to_csv(query, conn_string, out_path)


def export_produtos_distintos_por_dia(conn_string, out_path):
    query = """
        SELECT "NUM_ANOMESDIA", COUNT(DISTINCT "COD_ID_PRODUTO") AS qtd_produtos_distintos
        FROM vendas
        GROUP BY "NUM_ANOMESDIA"
        ORDER BY "NUM_ANOMESDIA"
    """
    run_query_to_csv(query, conn_string, out_path)


def export_produtos_maior_desconto(conn_string, out_path):
    query = """
        SELECT "COD_ID_PRODUTO", SUM("VAL_VALOR_DESCONTO") AS total_desconto
        FROM vendas
        GROUP BY "COD_ID_PRODUTO"
        ORDER BY total_desconto DESC
    """
    run_query_to_csv(query, conn_string, out_path)


def export_melhor_dia_semana_top_clientes(conn_string, out_path):
    # Top 20 clientes por compras: para cada um, melhor dia da semana
    query = """
        WITH top_clientes AS (
            SELECT "COD_ID_CLIENTE", COUNT(*) AS total_compras
            FROM vendas
            GROUP BY "COD_ID_CLIENTE"
            ORDER BY total_compras DESC
            LIMIT 20
        ), vendas_semana AS (
            SELECT v."COD_ID_CLIENTE",
                   TO_CHAR(TO_DATE(v."NUM_ANOMESDIA"::text, 'YYYYMMDD'), 'Day') AS dia_semana,
                   COUNT(*) AS qtd
            FROM vendas v
            JOIN top_clientes t ON v."COD_ID_CLIENTE" = t."COD_ID_CLIENTE"
            GROUP BY v."COD_ID_CLIENTE", dia_semana
        )
        SELECT "COD_ID_CLIENTE", TRIM(dia_semana) as melhor_dia_semana, qtd
        FROM (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY "COD_ID_CLIENTE" ORDER BY qtd DESC) as rn
            FROM vendas_semana
        ) t
        WHERE rn = 1
        ORDER BY qtd DESC
    """
    run_query_to_csv(query, conn_string, out_path)
