import logging
import sys
from datetime import datetime, timezone

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

# ============================================================
# JOB PARAMETERS
# ============================================================
#
#   --TABLE_NAME          meta_alfabetizacao_brasil
#   --BUCKET_BRONZE       s3://bucket/bronze
#   --BUCKET_SILVER       s3://bucket/silver

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'TABLE_NAME',
    'BUCKET_BRONZE',
    'BUCKET_SILVER',
])
JOB_NAME = args['JOB_NAME']
TABLE_NAME = args['TABLE_NAME']
BUCKET_BRONZE = args['BUCKET_BRONZE']
BUCKET_SILVER = args['BUCKET_SILVER']


# ============================================================
# VARIABLES
# ============================================================

def transform_uf(dataframe: DataFrame) -> DataFrame:
    dataframe = (dataframe
                    .dropDuplicates(['ano', 'sigla_uf', 'serie', 'rede'])
                    .withColumn("ano", F.col("ano").cast("int"))
                    .withColumn("serie", F.col("serie").cast("int"))
                    .withColumn("rede", F.col("rede").cast("int"))
                    .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double"))
                    .withColumn("media_portugues", F.col("media_portugues").cast("double"))
        )
    return dataframe


def transform_municipio(dataframe: DataFrame) -> DataFrame:
    dataframe = (dataframe
                    .dropDuplicates(['ano', 'id_municipio', 'serie', 'rede'])
                    .withColumn("ano", F.col("ano").cast("int"))
                    .withColumn("id_municipio", F.col("id_municipio").cast("int"))
                    .withColumn("serie", F.col("serie").cast("int"))
                    .withColumn("rede", F.col("rede").cast("int"))
                    .withColumn("taxa_alfabetizacao", F.col("taxa_alfabetizacao").cast("double"))
                    .withColumn("media_portugues", F.col("media_portugues").cast("double"))
        )
    return dataframe


def transform_alunos(dataframe: DataFrame) -> DataFrame:
    dataframe = (dataframe
                    .dropDuplicates(['ano', 'id_aluno'])
                    .withColumn("ano", F.col("ano").cast("int"))
                    .withColumn("id_municipio", F.col("id_municipio").cast("int"))
                    .withColumn("id_escola", F.col("id_escola").cast("int"))
                    .withColumn("id_aluno", F.col("id_aluno").cast("int"))
                    .withColumn("caderno", F.col("caderno").cast("int"))
                    .withColumn("serie", F.col("serie").cast("int"))
                    .withColumn("rede", F.col("rede").cast("int"))
                    .withColumn("presenca", F.col("presenca").cast("int"))
                    .withColumn("preenchimento_caderno", F.col("preenchimento_caderno").cast("int"))
                    .withColumn("alfabetizado", F.col("alfabetizado").cast("int"))
                    .withColumn("proficiencia", F.col("proficiencia").cast("double"))
                    .withColumn("peso_aluno", F.col("peso_aluno").cast("double"))
                    .withColumn('alfabetizado', F.when(
                        F.col('proficiencia').isNotNull(),
                        (F.col('proficiencia') >= 743.0).cast('integer')
                    ))
        )
    return dataframe


def transform_dicionario(dataframe: DataFrame) -> DataFrame:
    dataframe = dataframe.withColumn("chave", F.col("chave").cast("int"))
    return dataframe


def transform_meta_alfabetizacao_brasil(dataframe: DataFrame) -> DataFrame:
    dataframe = (dataframe
                    .dropDuplicates(['ano', 'rede'])
                    .withColumn("ano", F.col("ano").cast("int"))
        )

    columns_to_typing = ['taxa_alfabetizacao','meta_alfabetizacao_2024','meta_alfabetizacao_2025','meta_alfabetizacao_2026','meta_alfabetizacao_2027','meta_alfabetizacao_2028','meta_alfabetizacao_2029','meta_alfabetizacao_2030','percentual_participacao']
    for col in columns_to_typing:
        dataframe = dataframe.withColumn(col, F.col(col).cast("double"))
    
    return dataframe


def transform_meta_alfabetizacao_municipio(dataframe: DataFrame) -> DataFrame:
    dataframe = (dataframe
                    .dropDuplicates(['ano', 'id_municipio', 'rede'])
                    .withColumn("ano", F.col("ano").cast("int"))
                    .withColumn("id_municipio", F.col("id_municipio").cast("int"))
                    .withColumn("nivel_alfabetizacao", F.col("nivel_alfabetizacao").cast("int"))
                )

    columns_to_typing = ['taxa_alfabetizacao','meta_alfabetizacao_2024','meta_alfabetizacao_2025','meta_alfabetizacao_2026','meta_alfabetizacao_2027','meta_alfabetizacao_2028','meta_alfabetizacao_2029','meta_alfabetizacao_2030','percentual_participacao']
    for col in columns_to_typing:
        dataframe = dataframe.withColumn(col, F.col(col).cast("double"))

    return dataframe


def transform_meta_alfabetizacao_uf(dataframe: DataFrame) -> DataFrame:
    dataframe = (dataframe
                    .dropDuplicates(['ano', 'sigla_uf', 'rede'])
                    .withColumn("ano", F.col("ano").cast("int"))
                )
    
    columns_to_typing = ['taxa_alfabetizacao','meta_alfabetizacao_2024','meta_alfabetizacao_2025','meta_alfabetizacao_2026','meta_alfabetizacao_2027','meta_alfabetizacao_2028','meta_alfabetizacao_2029','meta_alfabetizacao_2030','percentual_participacao']
    for col in columns_to_typing:
        dataframe = dataframe.withColumn(col, F.col(col).cast("double"))

    return dataframe


TABLES = [
    {
        "name": "uf",
        "transformer": transform_uf,
    },
    {
        "name": "municipio",
        "transformer": transform_municipio,
    },
    {
        "name": "alunos",
        "transformer": transform_alunos,
        "checks": [
            {"column": "proficiencia", "rule": "range(0,1000)", "force_fail": True}
        ]
    },
    {
        "name": "dicionario",
        "transformer": transform_dicionario,
    },
    {
        "name": "meta_alfabetizacao_brasil",
        "transformer": transform_meta_alfabetizacao_brasil,
    },
    {
        "name": "meta_alfabetizacao_municipio",
        "transformer": transform_meta_alfabetizacao_municipio,
    },
    {
        "name": "meta_alfabetizacao_uf",
        "transformer": transform_meta_alfabetizacao_uf,
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

def extract_from_bucket(spark_session: SparkSession, bucket_bronze: str, table_name: str) -> DataFrame:
    """Retrieve data from bronze bucket"""

    path = f"{bucket_bronze}/{table_name}"
    dataframe = spark_session.read.parquet(path)
    return dataframe


def transform(dataframe: DataFrame, transformer: callable) -> DataFrame:
    """Transform the extracted data using the corresponding transformer function."""

    transformed_dataframe = transformer(dataframe)
    transformed_dataframe = transformed_dataframe.withColumn("_ingested_at", F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")))
    return transformed_dataframe


def quality_check(dataframe: DataFrame, table: dict[str, str]) -> None:
    """Perform quality checks on the DataFrame."""

    if 'checks' in table and len(table['checks']) > 0:
        for check in table['checks']:
            column = check['column']
            rule = check['rule']
            force_fail = check.get('force_fail', False)
            
            if "range" in rule:
                range_values = rule.replace("range(", "").replace(")", "").split(",")
                min_value = float(range_values[0])
                max_value = float(range_values[1])
                out_of_range_count = dataframe.filter((F.col(column) < min_value) | (F.col(column) > max_value)).count()
                if out_of_range_count > 0:
                    message = f"Quality check failed: Column '{column}' contains {out_of_range_count} values out of range [{min_value}, {max_value}]."
                    if force_fail:
                        raise ValueError(message)
                    else:
                        log.warning(message)


def load(dataframe: DataFrame, bucket_silver: str, table_name: str) -> str:
    """Load the DataFrame into S3 in Parquet format, partitioned by year."""

    path = f"{bucket_silver}/{table_name}"
    dataframe.write.partitionBy("_year").mode("overwrite").parquet(path)
    return path


# ============================================================
# EXECUTION
# ============================================================

log.info("=" * 60)
log.info(f"JOB_NAME : {JOB_NAME}")
log.info(f"TABLE_NAME : {TABLE_NAME}")
log.info(f"BUCKET_BRONZE : {BUCKET_BRONZE}")
log.info(f"BUCKET_SILVER : {BUCKET_SILVER}")
log.info("=" * 60)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

table = next((t for t in TABLES if t["name"] == TABLE_NAME), None)
if table is None:
    raise SystemError(f"Table {TABLE_NAME} not found in TABLES configuration.")

dataframe = extract_from_bucket(spark, BUCKET_BRONZE, table['name'])
log.info(f"Fetched {dataframe.count()} rows from bucket {BUCKET_BRONZE}.")

dataframe_transformed = transform(dataframe, table['transformer'])
log.info(f"Transformed dataframe with {dataframe_transformed.count()} rows and {len(dataframe_transformed.columns)} columns.")

quality_check(dataframe_transformed, table)
log.info(f"Quality checks passed for table {table['name']}.")

load_path = load(dataframe_transformed, BUCKET_SILVER, table['name'])
log.info(f"Transformed dataframe saved to {load_path}.")

job.commit()
