import json
import boto3

# Inicializa o cliente do Kinesis fora do handler (boas práticas para reutilização de conexões)
kinesis_client = boto3.client('kinesis')

def lambda_handler(event, context):
    stream_name = 'fiap-kinesis'
    
    # Dados que você quer enviar
    payload = {
        "id": "12345",
        "status": "sucesso",
        "mensagem": "Dados processados pela Lambda"
    }
    
    try:
        response = kinesis_client.put_record(
            StreamName=stream_name,
            Data=json.dumps(payload), # O Kinesis exige os dados em formato de bytes/string JSON
            PartitionKey='chave-de-particao-1' # Define em qual shard o dado vai cair
        )
        
        print(f"Registro enviado com sucesso. SequenceNumber: {response['SequenceNumber']}")
        
        return {
            'statusCode': 200,
            'body': json.dumps('Dado enviado ao Kinesis com sucesso!')
        }
        
    except Exception as e:
        print(f"Erro ao enviar para o Kinesis: {str(e)}")
        raise e