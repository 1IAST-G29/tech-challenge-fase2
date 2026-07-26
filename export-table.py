from pathlib import Path

from google.cloud import bigquery
from google.oauth2 import service_account

project_id = "pos-tech-ai-scientist"

# Caminho para o arquivo JSON da sua Conta de Serviço
caminho_json = "pos-tech-ai-scientist-916d921c977d.json"

# Carrega as credenciais a partir do arquivo
credenciais = service_account.Credentials.from_service_account_file(caminho_json)

# Inicializa o cliente BigQuery injetando as credenciais diretamente
client = bigquery.Client(credentials=credenciais, project=project_id)
Path("downloads").mkdir(parents=True, exist_ok=True)

id_tabela = "basedosdados.br_inep_avaliacao_alfabetizacao.uf"

# Busca a tabela e converte diretamente para um DataFrame do pandas
df = client.list_rows(id_tabela).to_dataframe()

# Salva o arquivo localmente no formato desejado
df.to_csv("downloads/uf2.csv", index=False)
# Para salvar em Excel: df.to_excel("dados_tabela.xlsx", index=False)

print("Download concluído com sucesso!")