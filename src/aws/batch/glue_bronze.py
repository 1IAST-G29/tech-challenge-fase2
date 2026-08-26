import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import DataFrame

# ============================================================
# JOB PARAMETERS
# ============================================================
#
#   --BIGQUERY_CONNECTION bigquery-connection
#   --GCP_PROJECT_ID      gcp-project-id
#   --TABLE_NAME          meta_alfabetizacao_brasil
#   --BUCKET_BRONZE       s3://bucket/bronze

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'BIGQUERY_CONNECTION',
    'GCP_PROJECT_ID',
    'TABLE_NAME',
    'BUCKET_BRONZE',
])
JOB_NAME = args['JOB_NAME']
BIGQUERY_CONNECTION = args['BIGQUERY_CONNECTION']
GCP_PROJECT_ID = args['GCP_PROJECT_ID']
TABLE_NAME = args['TABLE_NAME']
BUCKET_BRONZE = args['BUCKET_BRONZE']


# ============================================================
# VARIABLES
# ============================================================

TABLES = [
    {
        "name": "uf",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.uf",
        "checks": [
            {"column": "ano", "rule": "not_null", "critico": True},
        ]
    },
    {
        "name": "municipio",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.municipio",
        "checks": [
            {"tipo": "min_count", "valor": 1,      "critico": True},
            {"tipo": "not_null",  "coluna": "id",   "critico": True},
            {"tipo": "not_null",  "coluna": "nome","critico": True},
        ]
    },
    {
        "name": "alunos",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.alunos",
        "checks": [
            {"tipo": "min_count", "valor": 1,      "critico": True},
            {"tipo": "not_null",  "coluna": "id",   "critico": True},
            {"tipo": "not_null",  "coluna": "nome","critico": True},
        ]
    },
    {
        "name": "dicionario",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario",
        "checks": [
            {"tipo": "min_count", "valor": 1,      "critico": True},
            {"tipo": "not_null",  "coluna": "id",   "critico": True},
            {"tipo": "not_null",  "coluna": "nome","critico": True},
        ]
    },
    {
        "name": "meta_alfabetizacao_brasil",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil",
        "checks": [
            {"tipo": "min_count", "valor": 1,      "critico": True},
            {"tipo": "not_null",  "coluna": "id",   "critico": True},
            {"tipo": "not_null",  "coluna": "nome","critico": True},
        ]
    },
    {
        "name": "meta_alfabetizacao_municipio",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio",
        "checks": [
            {"tipo": "min_count", "valor": 1,      "critico": True},
            {"tipo": "not_null",  "coluna": "id",   "critico": True},
            {"tipo": "not_null",  "coluna": "nome","critico": True},
        ]
    },
    {
        "name": "meta_alfabetizacao_uf",
        "path": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf",
        "checks": [
            {"tipo": "min_count", "valor": 1,      "critico": True},
            {"tipo": "not_null",  "coluna": "id",   "critico": True},
            {"tipo": "not_null",  "coluna": "nome","critico": True},
        ]
    }
]


# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%SZ",
)
log = logging.getLogger(__name__)


# ============================================================
# FUNCTIONS
# ============================================================

def extract_from_bigquery(glue_context: GlueContext, connection_name: str, parent_project: str, table_id: str) -> DataFrame:
    """Extract data from BigQuery and return it as a DataFrame."""

    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="bigquery",
        connection_options={
            "connectionName": connection_name,
            "parentProject": parent_project,
            "table": table_id
        },
        transformation_ctx="bigquery_read")
    return dynamic_frame.toDF()


def transform(dataframe: DataFrame, job_name: str) -> DataFrame:
    """Transform the DataFrame by adding ingestion timestamp, job name, and year columns."""

    year = F.year(F.current_date())
    if "ano" in dataframe.columns:
        year = F.col("ano")

    dataframe = (
        dataframe
        .withColumn("_ingestion_timestamp", F.current_timestamp())
        .withColumn("_job_name", F.lit(job_name))
        .withColumn("_year", year)
    )
    return dataframe


def quality_check(dataframe: DataFrame, table: dict[str, str]) -> None:
    """Perform quality checks on the DataFrame."""


def load(dataframe: DataFrame, bucket_bronze: str, table_id: str) -> str:
    """Load the DataFrame into S3 in Parquet format, partitioned by year."""

    path = f"{bucket_bronze}/{table_id}"
    dataframe.write.partitionBy("_year").mode("overwrite").parquet(path)
    return path


# ============================================================
# EXECUTION
# ============================================================

log.info("=" * 60)
log.info(f"JOB_NAME : {JOB_NAME}")
log.info(f"BIGQUERY_CONNECTION : {BIGQUERY_CONNECTION}")
log.info(f"GCP_PROJECT_ID : {GCP_PROJECT_ID}")
log.info(f"TABLE_NAME : {TABLE_NAME}")
log.info(f"BUCKET_BRONZE : {BUCKET_BRONZE}")
log.info("=" * 60)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

table = next((t for t in TABLES if t["name"] == TABLE_NAME), None)
if table is None:
    raise SystemError(f"Table {TABLE_NAME} not found in TABLES configuration.")

dataframe = extract_from_bigquery(glueContext, BIGQUERY_CONNECTION, GCP_PROJECT_ID, table['path'])
log.info(f"Fetched {dataframe.count()} rows from table {table['path']}.")

dataframe_transformed = transform(dataframe, JOB_NAME)
log.info(f"Transformed dataframe with {dataframe_transformed.count()} rows and {len(dataframe_transformed.columns)} columns.")

quality_check(dataframe_transformed, table)

load_path = load(dataframe_transformed, BUCKET_BRONZE, table['path'])
log.info(f"Transformed dataframe saved to {load_path}.")

job.commit()
