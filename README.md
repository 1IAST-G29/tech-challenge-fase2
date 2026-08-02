# Tech Challenge Fase 2

Este repositório contém rotinas de ETL em Python para extrair dados do BigQuery/BasedosDados e gravar arquivos Parquet em um bucket do Google Cloud Storage.

## Requisitos

- Python 3.13.x
- `pip` disponível no Python escolhido
- Conta GCP com um projeto ativo
- Bucket do Google Cloud Storage para escrita dos arquivos
- Credenciais de service account em JSON quando a execução for local

As dependências do projeto estão declaradas em [pyproject.toml](/Users/pedrolavor/Workspace/tech-challenge-fase2/pyproject.toml), no bloco `[project].dependencies`.

## 1. Instalar e usar Python 3.13

O projeto deve ser executado com Python 3.13. Se você ainda não tem essa versão instalada, escolha uma das opções abaixo.

### Opção A: instalador oficial

Baixe e instale o Python 3.13 pelo site oficial do Python. Depois confirme a versão:

```bash
python3.13 --version
```

### Opção B: `pyenv`

Se você usa `pyenv`, instale e selecione a versão 3.13 no diretório do projeto:

```bash
pyenv install 3.13.5
cd /caminho/do/projeto
pyenv local 3.13.5
python --version
```

Se o comando `python --version` não mostrar 3.13.x dentro da pasta do projeto, o ambiente ainda não está apontando para a versão correta.

### Alterar a versão de um ambiente existente

Se você já criou uma virtualenv com outra versão do Python, recrie a pasta `.venv` usando o interpretador 3.13. O ambiente antigo não muda de versão sozinho.

```bash
deactivate  # se a virtualenv estiver ativa
rm -rf .venv
python3.13 -m venv .venv
```

## 2. Criar e ativar a `venv`

Depois de garantir que o interpretador é o Python 3.13 correto, crie a virtualenv a partir dele.

```bash
cd /Users/pedrolavor/Workspace/tech-challenge-fase2
python3.13 -m venv .venv
source .venv/bin/activate
python --version
```

O comando `python --version` dentro da `venv` deve continuar mostrando 3.13.x. Essa validação é importante porque a `venv` herda exatamente o interpretador usado na criação.

## 3. Instalar as dependências

Como este repositório declara as dependências no `pyproject.toml`, você pode instalar o ambiente com diferentes ferramentas. Escolha uma abordagem e mantenha ela consistente no projeto.

### Com `pip`

Depois de ativar a `venv`, instale o projeto a partir do `pyproject.toml`:

```bash
python -m pip install --upgrade pip
pip install -e .
```

### Com `poetry`

Se você preferir Poetry, use o Python 3.13 e instale as dependências declaradas no projeto:

```bash
poetry env use 3.13
poetry install
```

### Com `conda`

Com conda, crie um ambiente com Python 3.13 e depois instale o projeto a partir do `pyproject.toml` dentro dele:

```bash
conda create -n tech-challenge-fase2 python=3.13 pip
conda activate tech-challenge-fase2
python -m pip install --upgrade pip
pip install -e .
```

### Com `uv`

O `uv` consegue trabalhar diretamente com o `pyproject.toml` do repositório:

```bash
uv sync
```

Se quiser criar a `venv` explicitamente com o `uv`, use:

```bash
uv venv --python 3.13 .venv
source .venv/bin/activate
uv sync
```

Se você usa VS Code, selecione o interpretador da pasta `.venv` para garantir que o editor e o terminal usem o mesmo Python.

## 4. Como executar o ETL

Qualquer script Python dentro de `etl/` pode ser executado com o mesmo padrão.
Se novos ETLs forem adicionados no futuro, a forma de execução continua a mesma.

Arquivo existente hoje:

- `etl/etl_bronze.py`

Cada script pode aceitar parâmetros por linha de comando e também ler variáveis do ambiente. Se a mesma variável estiver nos dois lugares, o argumento da linha de comando tem prioridade.

Padrão de execução:

```bash
python etl/<nome_do_script>.py [argumentos]
```

### Exemplo com `.env`

Você pode copiar o arquivo de exemplo e preencher os valores reais:

```bash
cp .env.example .env
```

Depois ajuste o `.env` com os valores corretos e execute o script desejado sem argumentos:

```bash
python etl/<nome_do_script>.py
```

### Exemplo com argumentos

Use os argumentos que o script específico expõe. Nos scripts atuais, os exemplos são:

#### Bronze

```bash
python etl/etl_bronze.py \
	--gcp-billing-project-id SEU_PROJECT_ID \
	--bucket-bronze gs://seu-bucket/etl/bronze \
	--env local \
	--log-level debug
```

#### Silver

```bash
python etl/etl_silver.py \
	--gcp-billing-project-id SEU_PROJECT_ID \
	--bucket-path gs://seu-bucket/etl \
	--env local \
	--log-level debug
```

Observações importantes:

- O comportamento de saída depende do script executado.
- Novos scripts devem seguir o mesmo padrão de execução e documentação.
- Quando `ENV=local`, o ETL pode usar cache em `.data/extract_cache/` para reaproveitar extrações anteriores.

## 5. Configurar `.env` ou argumentos

O projeto usa `python-dotenv`, então as variáveis definidas em `.env` são carregadas automaticamente quando o script inicia.

### Exemplo de `.env`

```bash
ENV=local
LOG_LEVEL=debug
GCP_BILLING_PROJECT_ID=seu-project-id
GOOGLE_APPLICATION_CREDENTIALS=./credentials.json
BUCKET_BRONZE=gs://seu-bucket/etl/bronze
BUCKET_PATH=gs://seu-bucket/etl
```

### Significado das variáveis

- `ENV`: ambiente de execução. O valor `local` ativa o uso de cache local para extração.
- `LOG_LEVEL`: nível de log. Exemplos: `debug`, `info`, `warning`, `error`.
- `GCP_BILLING_PROJECT_ID`: projeto GCP usado para faturamento das consultas no BigQuery.
- `GOOGLE_APPLICATION_CREDENTIALS`: caminho para o JSON da service account.
- `BUCKET_BRONZE`: destino do ETL bronze.
- `BUCKET_PATH`: base de destino usada pelo ETL silver.

### Prioridade entre `.env` e argumentos

Se você informar um valor tanto no `.env` quanto na linha de comando, o argumento da linha de comando prevalece.

Exemplo:

```bash
python etl/etl_bronze.py --gcp-billing-project-id projeto-da-execucao
```

Nesse caso, o valor passado no comando substitui o que estiver definido em `GCP_BILLING_PROJECT_ID` no `.env`.

## 6. Requisitos no GCP

Para executar o ETL em um ambiente GCP válido, você precisa de:

- `GCP_BILLING_PROJECT_ID`: projeto que será cobrado pelas consultas ao BigQuery.
- `GOOGLE_APPLICATION_CREDENTIALS`: arquivo JSON de uma service account com permissão para acessar o BigQuery e gravar no bucket.
- `BUCKET_BRONZE` e/ou `BUCKET_PATH`: URI de bucket no formato `gs://...`.
- Bucket já criado no Google Cloud Storage.
- APIs do BigQuery e do Cloud Storage habilitadas no projeto.

### Permissões recomendadas

As permissões exatas podem variar conforme a política da sua organização, mas normalmente você vai precisar de algo equivalente a:

- `BigQuery Job User`
- `BigQuery Data Viewer` nas bases consultadas
- `Storage Object Creator` ou `Storage Object Admin` no bucket de destino

### Exemplo de credencial local

Se você estiver rodando localmente, a variável `GOOGLE_APPLICATION_CREDENTIALS` deve apontar para o arquivo JSON baixado da service account:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=./credentials.json
```

## 7. Fluxo sugerido de uso

1. Instale o Python 3.13.
2. Crie e ative a `venv` com o Python 3.13.
3. Instale as dependências com `pip install -e .`.
4. Configure o `.env` ou passe os argumentos na execução.
5. Execute `etl/etl_bronze.py` e depois `etl/etl_silver.py`.

Se quiser, eu também posso adaptar este README para um formato mais enxuto ou incluir uma seção de troubleshooting com erros comuns de `venv`, BigQuery e GCS.
