import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os
import gc


def export_indicadores_csv(indicadores, path_prefix):
    os.makedirs(os.path.dirname(path_prefix), exist_ok=True)
    for nome, df in indicadores.items():
        df.to_csv(f"{path_prefix}_{nome}.csv", index=False)
        del df
        gc.collect()


def export_vendas_parquet(vendas, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    vendas['ANO'] = pd.to_datetime(vendas['NUM_ANOMESDIA'], format='%Y%m%d').dt.year
    vendas['MES'] = pd.to_datetime(vendas['NUM_ANOMESDIA'], format='%Y%m%d').dt.month
    vendas['DIA'] = pd.to_datetime(vendas['NUM_ANOMESDIA'], format='%Y%m%d').dt.day
    for (ano, mes), df_group in vendas.groupby(['ANO', 'MES'], sort=False):
        out_path = os.path.join(output_dir, f"ano={ano}", f"mes={mes}")
        os.makedirs(out_path, exist_ok=True)
        pq.write_table(pa.Table.from_pandas(df_group, preserve_index=False),
                       os.path.join(out_path, "vendas.parquet"))
        del df_group
        gc.collect()


def export_divergencias_csv(divergencias, output_dir):
    os.makedirs(output_dir, exist_ok=True)
    for nome, arr in divergencias.items():
        pd.DataFrame(arr, columns=[nome]).to_csv(
            os.path.join(output_dir, f"{nome}.csv"), index=False)
        gc.collect()
