from pathlib import Path

from google.cloud import bigquery, storage
from google.oauth2 import service_account

project_id = "pos-tech-ai-scientist"

# Caminho para o arquivo JSON da sua Conta de Serviço
caminho_json = "pos-tech-ai-scientist-916d921c977d.json"

# Carrega as credenciais a partir do arquivo
credenciais = service_account.Credentials.from_service_account_file(caminho_json)

# Inicializa o cliente BigQuery injetando as credenciais diretamente
client = bigquery.Client(credentials=credenciais, project=project_id)
storage_client = storage.Client(credentials=credenciais, project=project_id)
Path("downloads").mkdir(parents=True, exist_ok=True)

id_tabela = "basedosdados.br_inep_avaliacao_alfabetizacao.uf"
df = client.list_rows(id_tabela).to_dataframe()

df.to_parquet("downloads/uf2.parquet", index=False)

# Testando a conexão
query_job = client.query("SELECT 1")
print(list(query_job.result()))