import os
import argparse
import logging
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone


# Set Argument Parser
parser = argparse.ArgumentParser(description="ETL Bronze Params")
parser.add_argument('--gcp-billing-project-id', type=str, help='GCP Billing Project ID. env(GCP_BILLING_PROJECT_ID)')
parser.add_argument('--bucket-bronze', type=str, help='Bronze Bucket URI. env(BUCKET_BRONZE)')
parser.add_argument('--bucket-silver', type=str, help='Silver Bucket URI. env(BUCKET_SILVER)')
parser.add_argument('--log-level', type=str, help='Log level. env(LOG_LEVEL)')
parser.add_argument('--env', type=str, help='Environment. env(ENV)')
parser.add_argument('--table', type=str, help='Table to extract. env(TABLE)')
args = parser.parse_args()

# Params Initialization
load_dotenv()
GCP_BILLING_PROJECT_ID = args.gcp_billing_project_id or os.getenv('GCP_BILLING_PROJECT_ID')
BUCKET_BRONZE = args.bucket_bronze or os.getenv('BUCKET_BRONZE')
BUCKET_SILVER = args.bucket_silver or os.getenv('BUCKET_SILVER')
LOG_LEVEL = args.log_level or os.getenv('LOG_LEVEL', 'INFO')
ENV = args.env or os.getenv('ENV')
TABLE = args.table or os.getenv('TABLE')

if not GCP_BILLING_PROJECT_ID:
    raise SystemError("GCP_BILLING_PROJECT_ID must be provided either as a command-line argument or an environment variable.")

if not BUCKET_BRONZE:
    raise SystemError("BUCKET_BRONZE must be provided either as a command-line argument or an environment variable.")

if not BUCKET_SILVER:
    raise SystemError("BUCKET_SILVER must be provided either as a command-line argument or an environment variable.")

if not TABLE:
    raise SystemError("TABLE must be provided either as a command-line argument or an environment variable.")


def transform_uf(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.drop_duplicates(subset=['ano', 'sigla_uf', 'serie', 'rede'])
    dataframe['ano'] = dataframe['ano'].astype(int)
    dataframe['serie'] = dataframe['serie'].astype(int)
    dataframe['rede'] = dataframe['rede'].astype(int)
    dataframe['taxa_alfabetizacao'] = pd.to_numeric(dataframe['taxa_alfabetizacao'], errors='coerce')
    dataframe['media_portugues'] = pd.to_numeric(dataframe['media_portugues'], errors='coerce')
    return dataframe

def transform_nome_municipio(dataframe: pd.DataFrame) -> pd.DataFrame:
    df_municipios = pd.read_csv('codigo_municipios.csv')
    df_municipios['id_municipio'] = df_municipios['id_municipio'].astype(int)
    dataframe = pd.merge(
    dataframe,
    df_municipios[['id_municipio', 'nome_municipio']],
    on='id_municipio',
    how='left'
    )
    columns = list(dataframe.columns)
    columns.remove('nome_municipio')
    idx = columns.index('id_municipio')
    columns.insert(idx + 1, 'nome_municipio')
    dataframe.drop(columns=['id_municipio'])
    return dataframe[columns]

def transform_municipio(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.drop_duplicates(subset=['ano', 'id_municipio', 'serie', 'rede'])
    dataframe['ano'] = dataframe['ano'].astype(int)
    dataframe['id_municipio'] = dataframe['id_municipio'].astype(int)
    dataframe['serie'] = dataframe['serie'].astype(int)
    dataframe['rede'] = dataframe['rede'].astype(int)
    dataframe['taxa_alfabetizacao'] = pd.to_numeric(dataframe['taxa_alfabetizacao'], errors='coerce')
    dataframe['media_portugues'] = pd.to_numeric(dataframe['media_portugues'], errors='coerce')
    dataframe = transform_nome_municipio(dataframe)
    return dataframe


def transform_alunos(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.drop_duplicates(subset=['ano', 'id_aluno'])
    dataframe['ano'] = dataframe['ano'].astype(int)
    dataframe['id_municipio'] = dataframe['id_municipio'].astype(int)
    dataframe['id_escola'] = dataframe['id_escola'].astype(int)
    dataframe['id_aluno'] = dataframe['id_aluno'].astype(int)
    dataframe['caderno'] = dataframe['caderno'].astype(int)
    dataframe['serie'] = dataframe['serie'].astype(int)
    dataframe['rede'] = dataframe['rede'].astype(int)
    dataframe['presenca'] = dataframe['presenca'].astype(int)
    dataframe['preenchimento_caderno'] = dataframe['preenchimento_caderno'].astype(int)
    dataframe['alfabetizado'] = dataframe['alfabetizado'].astype(int)
    dataframe['proficiencia'] = pd.to_numeric(dataframe['proficiencia'], errors='coerce')
    dataframe['peso_aluno'] = pd.to_numeric(dataframe['peso_aluno'], errors='coerce')
    valid_prof = dataframe['proficiencia'].notnull()
    dataframe.loc[valid_prof, 'alfabetizado'] = (dataframe.loc[valid_prof, 'proficiencia'] >= 743.0).astype(int)
    dataframe = transform_nome_municipio(dataframe)
    return dataframe



def transform_dicionario(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe['chave'] = dataframe['chave'].astype(int)
    return dataframe



def transform_meta_alfabetizacao_brasil(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.drop_duplicates(subset=['ano', 'rede'])
    dataframe['ano'] = dataframe['ano'].astype(int)
    columns_to_typing = ['taxa_alfabetizacao','meta_alfabetizacao_2024','meta_alfabetizacao_2025','meta_alfabetizacao_2026','meta_alfabetizacao_2027','meta_alfabetizacao_2028','meta_alfabetizacao_2029','meta_alfabetizacao_2030','percentual_participacao']
    for col in columns_to_typing:
        dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')
    return dataframe



def transform_meta_alfabetizacao_municipio(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.drop_duplicates(subset=['ano', 'id_municipio', 'rede'])
    dataframe['ano'] = dataframe['ano'].astype(int)
    dataframe['id_municipio'] = dataframe['id_municipio'].astype(int)
    dataframe['nivel_alfabetizacao'] = dataframe['nivel_alfabetizacao'].apply(lambda x: int(x) if pd.notnull(x) else None)
    columns_to_typing = ['taxa_alfabetizacao','meta_alfabetizacao_2024','meta_alfabetizacao_2025','meta_alfabetizacao_2026','meta_alfabetizacao_2027','meta_alfabetizacao_2028','meta_alfabetizacao_2029','meta_alfabetizacao_2030','percentual_participacao']
    for col in columns_to_typing:
        dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')
    dataframe = transform_nome_municipio(dataframe)
    return dataframe



def transform_meta_alfabetizacao_uf(dataframe: pd.DataFrame) -> pd.DataFrame:
    dataframe = dataframe.drop_duplicates(subset=['ano', 'sigla_uf', 'rede'])
    dataframe['ano'] = dataframe['ano'].astype(int)
    columns_to_typing = ['taxa_alfabetizacao','meta_alfabetizacao_2024','meta_alfabetizacao_2025','meta_alfabetizacao_2026','meta_alfabetizacao_2027','meta_alfabetizacao_2028','meta_alfabetizacao_2029','meta_alfabetizacao_2030','percentual_participacao']
    for col in columns_to_typing:
        dataframe[col] = pd.to_numeric(dataframe[col], errors='coerce')
    return dataframe
    

# Variables
TABLES = [
    {
        "name": "uf",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.uf",
        "transformer": transform_uf
    },
    {
        "name": "municipio",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.municipio",
        "transformer": transform_municipio
    },
    {
        "name": "alunos",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.alunos",
        "transformer": transform_alunos
    },
    {
        "name": "dicionario",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario",
        "transformer": transform_dicionario
    },
    {
        "name": "meta_alfabetizacao_brasil",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil",
        "transformer": transform_meta_alfabetizacao_brasil
    },
    {
        "name": "meta_alfabetizacao_municipio",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio",
        "transformer": transform_meta_alfabetizacao_municipio
    },
    {
        "name": "meta_alfabetizacao_uf",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf",
        "transformer": transform_meta_alfabetizacao_uf
    }
]

if TABLE not in map(lambda x: x["name"], TABLES):
    raise SystemError("TABLE must be one of the tables in TABLES.")

# Logger
logging.basicConfig(
    level=LOG_LEVEL.upper(),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)
log.debug(f"ENV: {ENV}, LOG_LEVEL: {LOG_LEVEL}, GCP_BILLING_PROJECT_ID: {GCP_BILLING_PROJECT_ID}, BUCKET_BRONZE: {BUCKET_BRONZE}, BUCKET_SILVER: {BUCKET_SILVER}, TABLE: {TABLE}")

# Functions
def extract(table: dict[str, str]) -> pd.DataFrame:
    """Retrieve data from bronze bucket"""
    file_path = f"{BUCKET_BRONZE}/{table['name']}.parquet"
    try:
        dataframe = pd.read_parquet(file_path)
        log.debug(f"Loaded cached data for table: {table['name']} from {file_path}")
        return dataframe
    except Exception as e:
        log.error(f"Error extracting data from {file_path}: {e}")
        raise e


def transform(table: dict[str, str], dataframe: pd.DataFrame) -> pd.DataFrame:
    """Transform the extracted data using the corresponding transformer function."""
    try:
        transformed_dataframe = table["transformer"](dataframe)
        transformed_dataframe['_env'] = ENV
        transformed_dataframe['_source_table'] = table['path']
        transformed_dataframe['_ingested_at'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        log.debug(f"Transformed data for table: {table['name']}")
        return transformed_dataframe
    except Exception as e:
        log.error(f"Error transforming data for table {table['name']}: {e}")
        raise e


def load(table: dict[str, str], dataframe: pd.DataFrame):
    """Load the extracted data into the target storage."""
    file_path = f"{BUCKET_SILVER}/{table['name']}.parquet"
    try:
        dataframe.to_parquet(file_path, index=False)
        log.debug(f"Data loaded for table: {table['name']} to {file_path}")
    except Exception as e:
        log.error(f"Error loading data for table {table['name']} to {file_path}: {e}")
        raise e


# Execution
if __name__ == "__main__":
    log.info(f"Starting ETL Silver process...")
    init_time = datetime.now()

    table = next((t for t in TABLES if t["name"] == TABLE), None)
    dataframe = extract(table)
    dataframe = transform(table, dataframe)
    load(table, dataframe)

    elapsed_time = datetime.now() - init_time
    log.info(f"ETL Silver process completed. Extracted data for {len(dataframe)} tables in {elapsed_time.total_seconds():.1f} seconds.")