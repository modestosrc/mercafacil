import zipfile
import pandas as pd
import os
from pathlib import Path


def extract_zip(zip_path, extract_to):
    os.makedirs(extract_to, exist_ok=True)
    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        zip_ref.extractall(extract_to)


# Le um arquivo json ou csv, normaliza os dados das colunas para upper case,
# e aplica os tipos de dados especificados no dicionário data_types.
def load_data(file_path, data_types):
    if file_path.endswith('.json'):
        df = pd.read_json(file_path)
        df.columns = df.columns.str.strip().str.upper()
        for col, tipo in data_types.items():
            col_up = col.strip().upper()
            if col_up in df.columns:
                if tipo.startswith("int"):
                    df[col_up] = pd.to_numeric(
                        df[col_up], errors='coerce', downcast="integer")
                elif tipo.startswith("float"):
                    df[col_up] = pd.to_numeric(
                        df[col_up], errors='coerce', downcast="float")
                elif tipo == "category":
                    df[col_up] = df[col_up].astype("category")
                elif tipo == "string":
                    df[col_up] = df[col_up].astype("string")
        return df

    # Caso seja CSV:
    with open(file_path, encoding="utf-8") as f:
        header = f.readline().strip().split(";")
    norm_types = {k.strip().upper(): v for k, v in data_types.items()}
    usecols = [col.strip().upper()
               for col in header if col.strip().upper() in norm_types]
    df = pd.read_csv(
        file_path,
        sep=';',
        dtype='string',
        usecols=usecols,
        low_memory=False
    )
    df.columns = df.columns.str.strip().str.upper()
    for col, tipo in norm_types.items():
        if col in df.columns:
            if tipo.startswith("int"):
                df[col] = pd.to_numeric(
                    df[col], errors='coerce', downcast="integer")
            elif tipo.startswith("float"):
                df[col] = pd.to_numeric(
                    df[col], errors='coerce', downcast="float")
            elif tipo == "category":
                df[col] = df[col].astype("category")
            elif tipo == "string":
                df[col] = df[col].astype("string")
    return df


def ingest(data, tmp_dir='/tmp/etl_data'):
    """
    Ingestão dos dados a partir de arquivos zip, extraindo e carregando em DataFrames.
    Args:
        data (dict): Dicionário com informações dos arquivos a serem processados.
        tmp_dir (str): Diretório temporário para extração dos arquivos.
    Returns:
        dict: Dicionário com DataFrames carregados, onde as chaves são os nomes dos datasets.
    """
    dataframes = {}
    for name, item in data.items():
        zip_path = item['path']
        # FIX: limpar o temp_dir em algum momento?
        extract_to = os.path.join(tmp_dir, Path(zip_path).stem)
        extract_zip(zip_path, extract_to)
        for file in os.listdir(extract_to):
            file_path = os.path.join(extract_to, file)
            df = load_data(file_path, item['types'])
            dataframes[name] = df
    return dataframes
