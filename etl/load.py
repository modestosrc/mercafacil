import pandas as pd
import io
import logging
from sqlalchemy import create_engine

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def create_table_from_df(df, table_name, engine):
    logging.info(f"Criando tabela '{
                 table_name}' no Postgres baseada no DataFrame...")
    df.head(0).to_sql(table_name, engine, if_exists='replace', index=False)


def fast_copy_to_postgres_in_batches(df, table_name, conn_string, batch_size=100_000):
    """
    Realiza o COPY da tabela para o Postgres em batches, usando StringIO.
    args:
        df (pd.DataFrame): DataFrame a ser copiado.
        table_name (str): Nome da tabela no Postgres.
        conn_string (str): String de conexão com o banco de dados Postgres.
        batch_size (int): Tamanho do batch para o COPY. Default é 100.000 linhas.
    Retorna:
        None
    """
    logging.info(f"Iniciando COPY da tabela '{
                 table_name}' para o Postgres em batches de {batch_size} linhas...")
    engine = create_engine(conn_string)
    conn = engine.raw_connection()
    cur = conn.cursor()
    total = len(df)
    for start in range(0, total, batch_size):
        end = min(start + batch_size, total)
        logging.info(f"COPY batch {start} até {end} de {total}...")
        batch_df = df.iloc[start:end]
        output = io.StringIO()
        batch_df.to_csv(output, sep='\t', header=False,
                        index=False, na_rep='\\N', quoting=3)
        output.seek(0)
        try:
            cur.copy_from(output, table_name, null='\\N', sep='\t')
            conn.commit()
            logging.info(f"COPY batch {start} até {end} OK!")
        except Exception as e:
            logging.error(f"Erro no COPY batch {start} até {end}: {e}")
            conn.rollback()
            raise
    cur.close()
    conn.close()
    logging.info(f"COPY da tabela '{table_name}' concluído com sucesso!")


def load_to_postgres(df, table_name, conn_string, batch_size=100_000):
    """
    Carrega o DataFrame para o Postgres, criando a tabela se necessário e usando COPY em batches.
    args:
        df (pd.DataFrame): DataFrame a ser carregado.
        table_name (str): Nome da tabela no Postgres.
        conn_string (str): String de conexão com o banco de dados Postgres.
        batch_size (int): Tamanho do batch para o COPY. Default é 100.000 linhas.
    returns:
        None
    """
    logging.info(f"Preparando para inserir '{table_name}' no Postgres...")
    engine = create_engine(conn_string)
    create_table_from_df(df, table_name, engine)
    fast_copy_to_postgres_in_batches(
        df, table_name, conn_string, batch_size=batch_size)


def replicate_clientes_to_mongo(df_clientes, mongo_uri, db_name, collection_name):
    """
    Replica os dados de clientes para uma coleção MongoDB.
    args:
        df_clientes (pd.DataFrame): DataFrame contendo os dados dos clientes.
        mongo_uri (str): URI de conexão com o MongoDB.
        db_name (str): Nome do banco de dados MongoDB.
        collection_name (str): Nome da coleção onde os dados serão inseridos.
    returns:
        None
    """
    from pymongo import MongoClient
    logging.info("Replicando clientes para MongoDB...")
    client = MongoClient(mongo_uri)
    db = client[db_name]
    collection = db[collection_name]
    collection.delete_many({})
    batch_size = 100_000
    records = df_clientes.to_dict('records')
    for i in range(0, len(records), batch_size):
        collection.insert_many(records[i:i+batch_size])
    logging.info("Dados de clientes replicados no MongoDB.")
