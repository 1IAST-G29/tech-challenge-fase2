import os
import argparse
import logging
import pandas as pd
from dotenv import load_dotenv
from google.cloud import bigquery
from datetime import datetime, timezone


# Argument Parser
parser = argparse.ArgumentParser(description="ETL Bronze Params")
parser.add_argument('--gcp-billing-project-id', type=str, help='GCP Billing Project ID. env(GCP_BILLING_PROJECT_ID)')
parser.add_argument('--bucket-bronze', type=str, help='Bronze Bucket URI. env(BUCKET_BRONZE)')
parser.add_argument('--log-level', type=str, help='Log level. env(LOG_LEVEL)')
parser.add_argument('--env', type=str, help='Environment. env(ENV)')
parser.add_argument('--table', type=str, help='Table to extract. env(TABLE)')
args = parser.parse_args()

# Params Initialization
load_dotenv()
GCP_BILLING_PROJECT_ID = args.gcp_billing_project_id or os.getenv('GCP_BILLING_PROJECT_ID')
BUCKET_BRONZE = args.bucket_bronze or os.getenv('BUCKET_BRONZE')
LOG_LEVEL = args.log_level or os.getenv('LOG_LEVEL', 'INFO')
ENV = args.env or os.getenv('ENV')
TABLE = args.table or os.getenv('TABLE')

if not GCP_BILLING_PROJECT_ID:
    raise SystemError("GCP_BILLING_PROJECT_ID must be provided either as a command-line argument or an environment variable.")

if not BUCKET_BRONZE:
    raise SystemError("BUCKET_BRONZE must be provided either as a command-line argument or an environment variable.")

if not TABLE:
    raise SystemError("TABLE must be provided either as a command-line argument or an environment variable.")

# Variables
TABLES = [
    {"name": "uf", "path": "basedosdados.br_inep_avaliacao_alfabetizacao.uf"},
    {"name": "municipio", "path": "basedosdados.br_inep_avaliacao_alfabetizacao.municipio"},
    {"name": "alunos", "path": "basedosdados.br_inep_avaliacao_alfabetizacao.alunos"},
    {"name": "dicionario", "path": "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario"},
    {"name": "meta_alfabetizacao_brasil", "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil"},
    {"name": "meta_alfabetizacao_municipio", "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio"},
    {"name": "meta_alfabetizacao_uf", "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf"}
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
log.debug(f"ENV: {ENV}, LOG_LEVEL: {LOG_LEVEL}, GCP_BILLING_PROJECT_ID: {GCP_BILLING_PROJECT_ID}, BUCKET_BRONZE: {BUCKET_BRONZE}, TABLE: {TABLE}")

# Functions
def extract(table: dict[str, str]) -> pd.DataFrame:
    """Extract basedosdados table from BigQuery and cache locally if env local."""
    table_name = table["name"]
    table_path = table["path"]
    
    try:
        dataframe = pd.DataFrame()
        cache_path = f".data/.cache_extract/{table_name}.parquet"

        if ENV == "local" and os.path.exists(cache_path):
            log.debug(f"Loading data from local cache: {cache_path}")
            dataframe = pd.read_parquet(cache_path)

        else:
            log.debug(f"Loading data from BigQuery: {table_path}")
            bigquery_client = bigquery.Client(project=GCP_BILLING_PROJECT_ID)
            dataframe = bigquery_client.list_rows(table_path).to_dataframe()

            if ENV == "local":
                os.makedirs(os.path.dirname(cache_path), exist_ok=True)
                dataframe.to_parquet(cache_path, index=False)
                log.debug(f"Cached data to local path: {cache_path}")

        dataframe['_env'] = ENV
        dataframe['_source_table'] = table_path
        dataframe['_ingested_at'] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        log.debug(f"Extracted {len(dataframe)} rows from table: {table_path}")
        return dataframe

    except Exception as e:
        log.error(f"Error extracting data from table {table_path}: {e}")
        raise e


def load(table: dict[str, str], dataframe: pd.DataFrame):
    """Load dataframe to bronze bucket."""
    file_path = f"{BUCKET_BRONZE}/{table['name']}.parquet"
    try:
        dataframe.to_parquet(file_path, index=False)
        log.debug(f"Dataframe loaded on {file_path}")
    except Exception as e:
        log.error(f"Error loading dataframe to {file_path}: {e}")
        raise e

# Execution
if __name__ == "__main__":
    log.info(f"Starting ETL Bronze process...")
    init_time = datetime.now()

    table = next((t for t in TABLES if t["name"] == TABLE), None)
    dataframe = extract(table)
    load(table, dataframe)

    elapsed_time = datetime.now() - init_time
    log.info(f"ETL Bronze process completed. Extracted data for {len(dataframe)} tables in {elapsed_time.total_seconds():.1f} seconds.")