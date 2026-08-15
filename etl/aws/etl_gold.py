import os
import sys
import logging
import pandas as pd
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job


## @params: [JOB_NAME, GCP_BILLING_PROJECT_ID, BUCKET_BRONZE, LOG_LEVEL, ENV, TABLE]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BUCKET_SILVER',
    'BUCKET_GOLD',
    'LOG_LEVEL',
    'ENV',
])


# Params Initialization
JOB_NAME = args['JOB_NAME']
BUCKET_SILVER = args['BUCKET_SILVER']
BUCKET_GOLD = args['BUCKET_GOLD']
LOG_LEVEL = args['LOG_LEVEL']
ENV = args['ENV']

logging.basicConfig(
    level=LOG_LEVEL.upper(),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)

# Params Initialization
if not BUCKET_SILVER:
    raise SystemError("BUCKET_SILVER must be provided either as a command-line argument or an environment variable.")

if not BUCKET_GOLD:
    raise SystemError("BUCKET_GOLD must be provided either as a command-line argument or an environment variable.")


# Logger
log.debug(f"ENV: {ENV}, LOG_LEVEL: {LOG_LEVEL}, BUCKET_SILVER: {BUCKET_SILVER}, BUCKET_GOLD: {BUCKET_GOLD}")

# Variables
NOW = datetime.now(timezone.utc)

def build_brazil_literacy_result():
    df_raw = pd.read_parquet(f"{BUCKET_SILVER}/meta_alfabetizacao_brasil.parquet")
    df = df_raw.copy()

    def _get_meta(row):
        col = f"meta_alfabetizacao_{int(row['ano'])}"
        return row.get(col, None)

    df['meta_alfabetizacao'] = df.apply(_get_meta, axis=1)

    df = df[['ano', 'rede', 'taxa_alfabetizacao', 'meta_alfabetizacao', 'percentual_participacao']].sort_values(by=['ano'])
    df.to_parquet(f"{BUCKET_GOLD}/alfabetizacao_brasil.parquet", index=False)


def build_state_literacy_result():
    df_raw = pd.read_parquet(f"{BUCKET_SILVER}/meta_alfabetizacao_uf.parquet")
    df = df_raw.copy()

    def _get_meta(row):
        col = f"meta_alfabetizacao_{int(row['ano'])}"
        return row.get(col, None)

    df['meta_alfabetizacao'] = df.apply(_get_meta, axis=1)

    df = df[['sigla_uf', 'ano', 'rede', 'taxa_alfabetizacao', 'meta_alfabetizacao', 'percentual_participacao']].sort_values(by=['sigla_uf', 'ano'])
    df.to_parquet(f"{BUCKET_GOLD}/alfabetizacao_uf.parquet", index=False)


def build_city_literacy_result():
    df_raw = pd.read_parquet(f"{BUCKET_SILVER}/meta_alfabetizacao_municipio.parquet")
    df = df_raw.copy()

    def _get_meta(row):
        col = f"meta_alfabetizacao_{int(row['ano'])}"
        return row.get(col, None)

    df['meta_alfabetizacao'] = df.apply(_get_meta, axis=1)

    df = df[['id_municipio', 'ano', 'rede', 'taxa_alfabetizacao', 'meta_alfabetizacao', 'nivel_alfabetizacao', 'percentual_participacao']].sort_values(by=['id_municipio', 'ano'])
    df.to_parquet(f"{BUCKET_GOLD}/alfabetizacao_municipio.parquet", index=False)


def build_state_result():
    df_dict = pd.read_parquet(f"{BUCKET_SILVER}/dicionario.parquet")
    df_state = pd.read_parquet(f"{BUCKET_SILVER}/uf.parquet")

    serie_map = df_dict.query("nome_coluna == 'serie' and id_tabela == 'uf'").set_index('chave')['valor'].to_dict()
    rede_map = df_dict.query("nome_coluna == 'rede' and id_tabela == 'uf'").set_index('chave')['valor'].to_dict()

    df_state['serie'] = df_state['serie'].map(serie_map)
    df_state['rede'] = df_state['rede'].map(rede_map)

    df = df_state[['sigla_uf', 'ano', 'serie', 'rede', 'taxa_alfabetizacao']].sort_values(by=['sigla_uf', 'ano'])
    df.to_parquet(f"{BUCKET_GOLD}/resultado_uf.parquet", index=False)


def build_city_result():
    df_dict = pd.read_parquet(f"{BUCKET_SILVER}/dicionario.parquet")
    df_city = pd.read_parquet(f"{BUCKET_SILVER}/municipio.parquet")

    serie_map = df_dict.query("nome_coluna == 'serie' and id_tabela == 'municipio'").set_index('chave')['valor'].to_dict()
    rede_map = df_dict.query("nome_coluna == 'rede' and id_tabela == 'municipio'").set_index('chave')['valor'].to_dict()

    df_city['serie'] = df_city['serie'].map(serie_map)
    df_city['rede'] = df_city['rede'].map(rede_map)

    df = df_city[['id_municipio', 'ano', 'serie', 'rede', 'taxa_alfabetizacao']].sort_values(by=['id_municipio', 'ano'])
    df.to_parquet(f"{BUCKET_GOLD}/resultado_municipio.parquet", index=False)


def build_students_result():
    df_dict = pd.read_parquet(f"{BUCKET_SILVER}/dicionario.parquet")
    df_students = pd.read_parquet(f"{BUCKET_SILVER}/alunos.parquet")

    serie_map = df_dict.query("nome_coluna == 'serie' and id_tabela == 'alunos'").set_index('chave')['valor'].to_dict()
    rede_map = df_dict.query("nome_coluna == 'rede' and id_tabela == 'alunos'").set_index('chave')['valor'].to_dict()
    alfabetizado_map = df_dict.query("nome_coluna == 'alfabetizado' and id_tabela == 'alunos'").set_index('chave')['valor'].to_dict()
    preenchimento_map = df_dict.query("nome_coluna == 'preenchimento_caderno' and id_tabela == 'alunos'").set_index('chave')['valor'].to_dict()
    presenca_map = df_dict.query("nome_coluna == 'presenca' and id_tabela == 'alunos'").set_index('chave')['valor'].to_dict()

    df_students['serie'] = df_students['serie'].map(serie_map)
    df_students['rede'] = df_students['rede'].map(rede_map)
    df_students['alfabetizado'] = df_students['alfabetizado'].map(alfabetizado_map)
    df_students['preenchimento_caderno'] = df_students['preenchimento_caderno'].map(preenchimento_map)
    df_students['presenca'] = df_students['presenca'].map(presenca_map)

    df = df_students[['id_aluno', 'ano', 'serie', 'rede', 'alfabetizado', 'presenca', 'preenchimento_caderno']].sort_values(by=['id_aluno', 'ano'])
    df.to_parquet(f"{BUCKET_GOLD}/resultado_alunos.parquet", index=False)


# Execution
if __name__ == "__main__":
    log.info(f"Starting ETL Gold process...")
    init_time = datetime.now()

    build_brazil_literacy_result()
    build_state_literacy_result()
    build_city_literacy_result()
    build_state_result()
    build_city_result()
    build_students_result()

    elapsed_time = datetime.now() - init_time
    log.info(f"ETL Gold process completed. Processed data in {elapsed_time.total_seconds():.1f} seconds.")