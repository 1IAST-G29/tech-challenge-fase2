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
from pyspark.sql.window import Window

# ============================================================
# JOB PARAMETERS
# ============================================================
#
#   --TABLE_NAME          meta_alfabetizacao_brasil
#   --BUCKET_SILVER       s3://bucket/silver
#   --BUCKET_GOLD         s3://bucket/gold
#   --BUCKET_GOLD         s3://bucket/assets

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'TABLE_NAME',
    'BUCKET_SILVER',
    'BUCKET_GOLD',
    'BUCKET_ASSETS',
])
JOB_NAME = args['JOB_NAME']
TABLE_NAME = args['TABLE_NAME']
BUCKET_SILVER = args['BUCKET_SILVER']
BUCKET_GOLD = args['BUCKET_GOLD']
BUCKET_ASSETS = args['BUCKET_ASSETS']




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

def resultado_uf_ano(spark_session: SparkSession, bucket_silver: str, bucket_gold: str, bucket_assets: str):
    meta_alfabetizacao_uf = spark_session.read.parquet(f"{bucket_silver}/meta_alfabetizacao_uf")

    meta_columns = [
        c for c in meta_alfabetizacao_uf.columns
        if c.startswith("meta_alfabetizacao_")
    ]

    meta_map = F.create_map(
        *[
            x
            for c in meta_columns
            for x in [
                F.lit(c.replace("meta_alfabetizacao_", "")),
                F.col(c)
            ]
        ]
    )

    meta_alfabetizacao_uf = (
        meta_alfabetizacao_uf
        .filter(F.col("taxa_alfabetizacao").isNotNull())
        .withColumn(
            "meta_alfabetizacao",
            F.element_at(
                meta_map,
                F.col("ano").cast("string")
            )
        )
    )

    (
        meta_alfabetizacao_uf
        .select(
            'sigla_uf',
            'ano',
            'taxa_alfabetizacao',
            'meta_alfabetizacao'
        )
        .orderBy(
            'sigla_uf',
            'ano'
        )
        .write.mode("overwrite").parquet(f"{bucket_gold}/resultado_uf_ano")
    )


def comparativo_anual_uf(spark_session: SparkSession, bucket_silver: str, bucket_gold: str, bucket_assets: str):
    meta_alfabetizacao_uf = spark_session.read.parquet(f"{bucket_silver}/meta_alfabetizacao_uf")

    window = Window.partitionBy("sigla_uf").orderBy("ano")

    (
        meta_alfabetizacao_uf
        .withColumn(
            "ano_inicio",
            F.col("ano")
        )
        .withColumn(
            "ano_final",
            F.lead("ano").over(window)
        )
        .withColumn(
            "resultado_inicio",
            F.col("taxa_alfabetizacao")
        )
        .withColumn(
            "resultado_final",
            F.lead("taxa_alfabetizacao").over(window)
        )
        .withColumn(
            "diferenca_resultado",
            F.col("resultado_final") - F.col("resultado_inicio")
        )
        .withColumn(
            "avaliacao",
            F.when(F.col("diferenca_resultado") > 0, "Aumentou")
            .when(F.col("diferenca_resultado") < 0, "Diminuiu")
            .otherwise("Se Manteve")
        )
        .filter(
            F.col("ano_final") == F.col("ano_inicio") + 1
        )
        .select(
            "sigla_uf",
            "ano_inicio",
            "ano_final",
            "resultado_inicio",
            "resultado_final",
            "diferenca_resultado",
            "avaliacao"
        )
        .write.mode("overwrite").parquet(f"{bucket_gold}/comparativo_anual_uf")
    )


estados = {
    "Acre": "AC",
    "Alagoas": "AL",
    "Amapá": "AP",
    "Amazonas": "AM",
    "Bahia": "BA",
    "Ceará": "CE",
    "Distrito Federal": "DF",
    "Espírito Santo": "ES",
    "Goiás": "GO",
    "Maranhão": "MA",
    "Mato Grosso": "MT",
    "Mato Grosso do Sul": "MS",
    "Minas Gerais": "MG",
    "Pará": "PA",
    "Paraíba": "PB",
    "Paraná": "PR",
    "Pernambuco": "PE",
    "Piauí": "PI",
    "Rio de Janeiro": "RJ",
    "Rio Grande do Norte": "RN",
    "Rio Grande do Sul": "RS",
    "Rondônia": "RO",
    "Roraima": "RR",
    "Santa Catarina": "SC",
    "São Paulo": "SP",
    "Sergipe": "SE",
}

regioes = {
    "Norte": ["AC", "AP", "AM", "PA", "RO", "RR", "TO"],
    "Nordeste": ["AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE"],
    "Centro-Oeste": ["DF", "GO", "MT", "MS"],
    "Sudeste": ["ES", "MG", "RJ", "SP"],
    "Sul": ["PR", "RS", "SC"],
}


def media_regiao_ano(spark_session: SparkSession, bucket_silver: str, bucket_gold: str, bucket_assets: str):
    estados_map = F.create_map(
        *[
            item
            for nome, sigla in estados.items()
            for item in [F.lit(nome), F.lit(sigla)]
        ]
    )

    regioes_map = F.create_map(
        *[
            item
            for regiao, siglas in regioes.items()
            for sigla in siglas
            for item in [F.lit(sigla), F.lit(regiao)]
        ]
    )

    divisao_territorial_brasileira = spark_session.read.csv(f"{bucket_assets}/divisao_territorial_brasileira.csv", header=True)
    divisao_territorial_brasileira = (
        divisao_territorial_brasileira
        .withColumn(
            "sigla_uf",
            estados_map[F.col("Nome_UF")]
        )
        .withColumn(
            "regiao",
            regioes_map[F.col("sigla_uf")]
        )
    )

    alunos = spark_session.read.parquet(f"{bucket_silver}/alunos")
    alunos_por_estado_ano = (
        alunos
        .withColumn(
            "UF",
            F.substring("id_municipio", 1, 2)
        )
        .groupBy("UF", "ano")
        .agg(
            F.count("id_aluno").alias("total_alunos")
        )
    )

    meta_alfabetizacao_uf = spark_session.read.parquet(f"{bucket_silver}/meta_alfabetizacao_uf")
    (
        meta_alfabetizacao_uf
            .join(
                divisao_territorial_brasileira.select("sigla_uf", "Nome_UF", "UF", "regiao"),
                on=["sigla_uf"],
                how="left"
            )
            .join(
                alunos_por_estado_ano,
                on=["UF", "ano"],
                how="left"
            )
            .withColumn("taxa_x_alunos", F.col("taxa_alfabetizacao") * F.col("total_alunos"))
            .groupBy("regiao", "ano")
            .agg(
                F.sum("taxa_x_alunos").alias("soma_taxa_x_alunos"),
                F.sum("total_alunos").alias("soma_total_alunos")
            )
            .withColumn(
                "media_regiao_ano",
                F.col("soma_taxa_x_alunos") / F.col("soma_total_alunos")
            )
            .select(
                "regiao",
                "ano",
                "media_regiao_ano",
            )
            .orderBy(
                "regiao",
                "ano"
            )
            .write.mode("overwrite").parquet(f"{bucket_gold}/media_regiao_ano")
    )


# ============================================================
# VARIABLES
# ============================================================

TABLES = [
    {
        "name": "resultado_uf_ano",
        "builder": resultado_uf_ano,
    },
    {
        "name": "comparativo_anual_uf",
        "builder": comparativo_anual_uf,
    },
    {
        "name": "media_regiao_ano",
        "builder": media_regiao_ano,
    },
]


# ============================================================
# EXECUTION
# ============================================================

log.info("=" * 60)
log.info(f"JOB_NAME : {JOB_NAME}")
log.info(f"TABLE_NAME : {TABLE_NAME}")
log.info(f"BUCKET_SILVER : {BUCKET_SILVER}")
log.info(f"BUCKET_GOLD : {BUCKET_GOLD}")
log.info(f"BUCKET_ASSETS : {BUCKET_ASSETS}")
log.info("=" * 60)

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(JOB_NAME, args)

table = next((t for t in TABLES if t["name"] == TABLE_NAME), None)
if table is None:
    raise SystemError(f"Table {TABLE_NAME} not found in TABLES configuration.")

builder = table['builder']
builder(spark, BUCKET_SILVER, BUCKET_GOLD, BUCKET_ASSETS)

job.commit()
