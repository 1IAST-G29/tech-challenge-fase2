import logging
import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StructField, StructType

# ============================================================
# JOB PARAMETERS
# ============================================================
#
#   --KINESIS_STREAM_ARN  arn:aws:kinesis:zone:id:stream/name
#   --BUCKET_GOLD         s3://bucket/gold

args = getResolvedOptions(
    sys.argv,
    [
        "JOB_NAME",
        "KINESIS_STREAM_ARN",
        "BUCKET_GOLD"
    ]
)

JOB_NAME = args["JOB_NAME"]
KINESIS_STREAM_ARN = args["KINESIS_STREAM_ARN"]
BUCKET_GOLD = args["BUCKET_GOLD"]


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
# SCHEMA
# ============================================================

ALUNOS_SCHEMA = StructType([
    StructField("ano", IntegerType(), True),
    StructField("id_municipio", LongType(), True),
    StructField("id_escola", LongType(), True),
    StructField("id_aluno", LongType(), True),
    StructField("caderno", IntegerType(), True),
    StructField("serie", IntegerType(), True),
    StructField("rede", IntegerType(), True),
    StructField("presenca", IntegerType(), True),
    StructField("preenchimento_caderno", IntegerType(), True),
    StructField("alfabetizado", IntegerType(), True),
    StructField("proficiencia", DoubleType(), True),
    StructField("peso_aluno", DoubleType(), True),
])


# ============================================================
# EXECUTION
# ============================================================

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

dataframe_kinesis = glueContext.create_data_frame.from_options(
    connection_type="kinesis",
    connection_options={
        "typeOfData": "kinesis",
        "streamARN": KINESIS_STREAM_ARN,
        "classification": "json",
        "startingPosition": "earliest",
        "inferSchema": "true"
    }, 
    transformation_ctx="dataframe_kinesis")

def processBatch(data_frame, batchId):
    json_column = "$json$data_infer_schema$_temporary$"
    dataframe_alunos = (
        data_frame
        .withColumn(
            "payload",
            F.from_json(
                F.col(json_column),
                ALUNOS_SCHEMA
            )
        )
        .select("payload.*")
    )

    if (dataframe_alunos.count() > 0):

        year = F.year(F.current_date())
        if "ano" in dataframe_alunos.columns:
            year = F.col("ano")

        dataframe_alunos_analfabetos = (
            dataframe_alunos
            .filter(F.col("alfabetizado") == 0)
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_job_name", F.lit(JOB_NAME))
            .withColumn("_source", F.lit("kinesis"))
            .withColumn("_year", year)
        )

        dataframe_alunos_analfabetos.write.partitionBy("_year").mode("append").parquet(f"{BUCKET_GOLD}/alunos_analfabetos")


glueContext.forEachBatch(
    frame = dataframe_kinesis,
    batch_function = processBatch,
    options = {"windowSize": "100 seconds", "checkpointLocation": args["TempDir"] + "/" + args["JOB_NAME"] + "/checkpoint/"})
    
job.commit()