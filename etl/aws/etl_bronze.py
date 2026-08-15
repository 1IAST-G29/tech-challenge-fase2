import os
import sys
import logging
import pandas as pd
import boto3
from google.cloud import bigquery
from datetime import datetime, timezone
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


## @params: [JOB_NAME, GCP_BILLING_PROJECT_ID, BUCKET_BRONZE, LOG_LEVEL, ENV, TABLE]
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'GCP_BILLING_PROJECT_ID',
    'GCP_CREDENTIALS',
    'BUCKET_BRONZE',
    'LOG_LEVEL',
    'ENV',
    'TABLE'
])

# Params Initialization
JOB_NAME = args['JOB_NAME']
GCP_BILLING_PROJECT_ID = args['GCP_BILLING_PROJECT_ID']
GCP_CREDENTIALS = args['GCP_CREDENTIALS']
BUCKET_BRONZE = args['BUCKET_BRONZE']
LOG_LEVEL = args['LOG_LEVEL']
ENV = args['ENV']
TABLE = args['TABLE']

if not GCP_BILLING_PROJECT_ID:
    raise SystemError("GCP_BILLING_PROJECT_ID must be provided either as a command-line argument or an environment variable.")

if not GCP_CREDENTIALS:
    raise SystemError("GCP_CREDENTIALS must be provided either as a command-line argument or an environment variable.")

if not BUCKET_BRONZE:
    raise SystemError("BUCKET_BRONZE must be provided either as a command-line argument or an environment variable.")

if not TABLE:
    raise SystemError("TABLE must be provided either as a command-line argument or an environment variable.")
    
# 2. Define o caminho local temporário para a credencial no container do Glue
local_creds_path = "/tmp/gcp_creds.json"

# 3. Baixa o arquivo de credenciais JSON do S3 para o disco local do Glue
s3_path = args['GCP_CREDENTIALS']
bucket_name = s3_path.split('/')[2]
s3_key = '/'.join(s3_path.split('/')[3:])

s3_client = boto3.client('s3')
s3_client.download_file(bucket_name, s3_key, local_creds_path)

# 4. Configura a variável de ambiente que o cliente do Google Cloud exige
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = local_creds_path



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
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

log.info(f"Starting ETL Bronze process...")
init_time = datetime.now()

table = next((t for t in TABLES if t["name"] == TABLE), None)
dataframe = extract(table)
load(table, dataframe)

elapsed_time = datetime.now() - init_time
log.info(f"ETL Bronze process completed. Extracted data for {len(dataframe)} tables in {elapsed_time.total_seconds():.1f} seconds.")
job.commit()