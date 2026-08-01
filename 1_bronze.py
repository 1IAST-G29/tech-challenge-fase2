from pathlib import Path
import logging
import hashlib
from datetime import datetime, timezone

from google.cloud import bigquery, storage
from google.oauth2 import service_account

project_id = "pos-tech-ai-scientist"
caminho_json = "pos-tech-ai-scientist-916d921c977d.json"
credenciais = service_account.Credentials.from_service_account_file(caminho_json)

TABLE_IDS = {
    "uf": "basedosdados.br_inep_avaliacao_alfabetizacao.uf",
    "municipio": "basedosdados.br_inep_avaliacao_alfabetizacao.municipio",
    # "alunos": "basedosdados.br_inep_avaliacao_alfabetizacao.alunos",
    "dicionario": "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario",
    "meta_alfabetizacao_brasil": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil",
    "meta_alfabetizacao_municipio": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio",
    "meta_alfabetizacao_uf": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf",
}

CHECKS = {
    "uf": [
        {"type": "not_null", "column": "ano", "stop": True},
        {"type": "not_null", "column": "sigla_uf", "stop": True},
        {"type": "not_null", "column": "serie", "stop": True},
        {"type": "not_null", "column": "rede", "stop": True},
        {"type": "not_null", "column": "taxa_alfabetizacao", "stop": True},
        {"type": "not_null", "column": "media_portugues", "stop": True},
        {"type": "not_null", "column": "proporcao_aluno_nivel_8", "stop": True},
    ]
}

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
# VARIÁVEIS
# ============================================================

JOB_NAME       = "etl_bronze"
INGESTION_TS   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
INGESTION_DATE = datetime.now(timezone.utc).strftime("%Y-%m-%d")
ano, mes, dia  = INGESTION_DATE.split("-")

log.info("=" * 60)
log.info(f"JOB       : {JOB_NAME}")
log.info(f"DATA      : {INGESTION_DATE}")
log.info("=" * 60)

# ============================================================
# FUNCTIONS
# ============================================================
def extract(table_ids):
    """
    Extract data from BigQuery tables and return as a dictionary of DataFrames.
    """
    client = bigquery.Client(credentials=credenciais, project=project_id)
    dataframes = {}
    for name, table_id in table_ids.items():
        log.debug(f"Extracting data from table: {table_id}")
        df = client.list_rows(table_id).to_dataframe()
        df['_source_name'] = table_id
        df["_ingested_at"] = INGESTION_TS
        df["_ingested_date"] = INGESTION_DATE
        df["_job_name"] = JOB_NAME
        df["_record_hash"] = df.drop(
            columns=[c for c in df.columns if c.startswith("_")],
            errors="ignore"
        ).apply(lambda r: hashlib.md5(str(r.values).encode()).hexdigest(), axis=1)
        dataframes[name] = df
        log.debug(f"Extracted {len(df)} rows from table: {table_id}")
    return dataframes


def quality_check(dataframes, checks):
    """
    Perform quality checks on the extracted data.
    """

    log.info(f"[BRONZE] Iniciando verificacoes | checks={len(checks)}")
    passed = not_passed = failed = 0

    for table, rules in checks.items():


        log.info(f"[BRONZE] Verificando tabela: {table} => {dataframes.keys()}")
        if table not in dataframes.keys():
            continue

        for rule in rules:
            type    = rule["type"]
            column  = rule.get("column")
            stop_process = rule.get("stop_process", True)
            ok      = False
            detail = ""

            try:
                df = dataframes[table]

                if type == "not_null":
                    print(df[column].head(1))
                    print()
                    nulls   = df[column].isnull().sum()
                    ok      = nulls == 0
                    detail = f"{nulls} null values found"
            except Exception as e:
                ok      = False
                detail = f"Erro: {e}"

            status = "PASS" if ok else ("FAIL" if stop_process else "WARN")
            if ok:
                passed += 1
                log.info(f"[BRONZE] {status} | {type} | column={column} | {detail}")
            else:
                not_passed += 1
                if stop_process:
                    failed += 1
                    log.error(f"[BRONZE] {status} | {type} | column={column} | {detail}")
                else:
                    log.warning(f"[BRONZE] {status} | {type} | column={column} | {detail}")

    score = round(passed / len(checks) * 100, 1)
    log.info(f"[BRONZE] Score={score}% | PASS={passed} FAIL={failed}")

    if failed > 0:
        raise Exception(f"[BRONZE] {failed} check(s) failed. STOP JOB!")


def load(dataframes):
    """
    Load the transformed data into Google Cloud Storage as Parquet files.
    """
    storage_client = storage.Client(credentials=credenciais, project=project_id)
    storage_bucket = storage_client.get_bucket("tech-challenge-fase2")
    downloads_dir = Path("downloads")
    downloads_dir.mkdir(parents=True, exist_ok=True)

    for name, df in dataframes.items():
        log.debug(f"Loading data for table: {name}")
        file_path = downloads_dir / f"{name}.parquet"
        df.to_parquet(file_path, index=False)
        storage_bucket.blob(f"{name}.parquet").upload_from_filename(file_path)
        log.debug(f"Loaded data for table: {name} to GCS as {name}.parquet")

# ============================================================
# EXECUTION
# ============================================================
log.info("[BRONZE] Init Extract")
dataframes = extract(TABLE_IDS)

log.info("[BRONZE] Check Quality")
quality_check(dataframes, CHECKS)

log.info("[BRONZE] Init Load")
load(dataframes)

log.info("[BRONZE] Finished ETL process")