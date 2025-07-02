import pandas as pd


def corrige_cod_id_venda_unico(df):
    def corrigir(row):
        cod_loja = str(row['COD_ID_LOJA'])
        venda_unico = str(row['COD_ID_VENDA_UNICO'])
        if '|' in venda_unico:
            cod_loja_unico, cod_cupom = venda_unico.split('|', 1)
            if cod_loja_unico != cod_loja:
                return f"{cod_loja}|{cod_cupom}"
            else:
                return venda_unico
        else:
            # TODO: Lidar com casos onde o formato não é esperado
            raise ValueError(
                f"Formato inválido para COD_ID_VENDA_UNICO: {venda_unico}")

    df['COD_ID_VENDA_UNICO'] = df.apply(corrigir, axis=1)
    return df


def transform(dataframes):
    """
    Realiza limpeza e enriquecimento dos DataFrames.
    Args:
        dataframes (dict): Dicionário contendo DataFrames carregados, onde as chaves são os nomes dos datasets.
    Returns:
        tuple: DataFrame de vendas corrigido e um dicionário com divergências encontradas.
    """
    vendas = dataframes.get("vendas")
    clientes = dataframes.get("clientes")
    produtos = dataframes.get("produtos")

    # Corrige o campo COD_ID_VENDA_UNICO
    vendas = corrige_cod_id_venda_unico(vendas)

    # Mantém apenas colunas essenciais para merge
    vendas = vendas.merge(
        produtos[['COD_ID_PRODUTO', 'DES_PRODUTO']],
        on='COD_ID_PRODUTO', how='left'
    )
    vendas = vendas.merge(
        clientes[['COD_ID_CLIENTE', 'NOM_NOME']],
        on='COD_ID_CLIENTE', how='left'
    )

    # Identifica divergências apenas nos IDs
    clientes_nao_encontrados = vendas.loc[vendas['NOM_NOME'].isnull(
    ), 'COD_ID_CLIENTE'].drop_duplicates().values
    produtos_nao_encontrados = vendas.loc[vendas['DES_PRODUTO'].isnull(
    ), 'COD_ID_PRODUTO'].drop_duplicates().values

    divergencias = {
        "clientes_nao_encontrados": clientes_nao_encontrados,
        "produtos_nao_encontrados": produtos_nao_encontrados
    }
    return vendas, divergencias
