from pathlib import Path

from google.cloud import bigquery, storage
from google.oauth2 import service_account

project_id = "pos-tech-ai-scientist"
caminho_json = "pos-tech-ai-scientist-916d921c977d.json"
credenciais = service_account.Credentials.from_service_account_file(caminho_json)

client = bigquery.Client(credentials=credenciais, project=project_id)
storage_client = storage.Client(credentials=credenciais, project=project_id)
storage_bucket = storage_client.get_bucket("tech-challenge-fase2")
downloads_dir = Path("downloads")
downloads_dir.mkdir(parents=True, exist_ok=True)

TABLE_IDS = {
    "uf": "basedosdados.br_inep_avaliacao_alfabetizacao.uf",
    "municipio": "basedosdados.br_inep_avaliacao_alfabetizacao.municipio",
    "alunos": "basedosdados.br_inep_avaliacao_alfabetizacao.alunos",
    "dicionario": "basedosdados.br_inep_avaliacao_alfabetizacao.dicionario",
    "meta_alfabetizacao_brasil": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_brasil",
    "meta_alfabetizacao_municipio": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_municipio",
    "meta_alfabetizacao_uf": "basedosdados.br_inep_avaliacao_alfabetizacao.meta_alfabetizacao_uf",
}

for name, table_id in TABLE_IDS.items():
    df = client.list_rows(table_id).to_dataframe()
    file_path = downloads_dir / f"{name}.parquet"
    df.to_parquet(file_path, index=False)
    storage_bucket.blob(f"{name}.parquet").upload_from_filename(file_path)