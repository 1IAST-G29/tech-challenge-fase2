import os
import argparse
import logging
import pandas as pd
from dotenv import load_dotenv
from datetime import datetime, timezone


# Load environment variables from .env file
load_dotenv()

# Set Argument Parser
parser = argparse.ArgumentParser(description="ETL Bronze Params")
parser.add_argument('--gcp-billing-project-id', type=str, help='GCP Billing Project ID. env(GCP_BILLING_PROJECT_ID)')
parser.add_argument('--bucket-silver', type=str, help='Silver Bucket URI. env(BUCKET_SILVER)')
parser.add_argument('--bucket-gold', type=str, help='Gold Bucket URI. env(BUCKET_GOLD)')
parser.add_argument('--log-level', type=str, help='Log level. env(LOG_LEVEL)')
parser.add_argument('--env', type=str, help='Environment. env(ENV)')
args = parser.parse_args()

# Params Initialization
GCP_BILLING_PROJECT_ID = args.gcp_billing_project_id or os.getenv('GCP_BILLING_PROJECT_ID')
BUCKET_SILVER = args.bucket_silver or os.getenv('BUCKET_SILVER')
BUCKET_GOLD = args.bucket_gold or os.getenv('BUCKET_GOLD')
LOG_LEVEL = args.log_level or os.getenv('LOG_LEVEL', 'INFO')
ENV = args.env or os.getenv('ENV')

if not GCP_BILLING_PROJECT_ID or not BUCKET_SILVER or not BUCKET_GOLD:
    raise SystemError("GCP_BILLING_PROJECT_ID and BUCKET_SILVER and BUCKET_GOLD must be provided either as command-line arguments or environment variables.")

# Logger
logging.basicConfig(
    level=LOG_LEVEL.upper(),
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)
log.debug(f"LOG_LEVEL: {LOG_LEVEL}, ENV: {ENV}, GCP_BILLING_PROJECT_ID: {GCP_BILLING_PROJECT_ID}, BUCKET_SILVER: {BUCKET_SILVER}, BUCKET_GOLD: {BUCKET_GOLD}")

# Variables
NOW = datetime.now(timezone.utc)
TABLES = [
    "basedosdados.br_inep_avaliacao_alfabetizacao.uf",
    "basedosdados.br_inep_avaliacao_alfabetizacao.municipio",
    "basedosdados.br_inep_avaliacao_alfabetizacao.alunos",
    "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario",
    "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil",
    "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio",
    "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf"
]

# Functions
def extract(tables: list) -> dict[str, pd.DataFrame]:
    """Retrieve data from bronze bucket"""
    dataframes = {}

    for table in tables:
        try:
            dataframes[table] = pd.read_parquet(f"{BUCKET_SILVER}/{table}.parquet")
            log.debug(f"Loaded cached data for table: {table} from {BUCKET_SILVER}/{table}.parquet")
        except Exception as e:
            log.error(f"Error extracting data from {BUCKET_SILVER}/{table}.parquet: {e}")
            raise e
        
    return dataframes


def load(dataframes: dict[str, pd.DataFrame]):
    """Load the extracted data into the target storage."""
    for table, df in dataframes.items():
        df.to_parquet(f"{BUCKET_GOLD}/{table}.parquet", index=False)
        log.debug(f"Data loaded for table: {table}")


# Execution
if __name__ == "__main__":
    log.info(f"Starting ETL Gold process...")
    init_time = datetime.now()
    extracted_data = extract(TABLES)
    load(extracted_data)
    elapsed_time = datetime.now() - init_time
    log.info(f"ETL Gold process completed. Extracted data for {len(extracted_data)} tables in {elapsed_time.total_seconds():.1f} seconds.")