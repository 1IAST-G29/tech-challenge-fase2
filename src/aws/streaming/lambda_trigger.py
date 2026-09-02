import json
import os
import random
import time

import boto3

kinesis = boto3.client("kinesis")
STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]


def generate_event():
    return {
        "ano": random.randint(2026, 2030),
        "id_municipio": random.randint(1100015, 5300108),
        "id_escola": random.randint(60000001, 60001000),
        "id_aluno": random.randint(11000001, 11001000),
        "caderno": random.randint(1, 50),
        "serie": 2,
        "rede": random.choice([2, 3, 4]),
        "presenca": random.choice([0, 1]),
        "preenchimento_caderno": random.choice([0, 1]),
        "alfabetizado": random.choice([0, 1]),
        "proficiencia": round(random.uniform(0, 1000), 2),
        "peso_aluno": round(random.uniform(0, 1), 4),
    }


def lambda_handler(event, context):
    number_of_events = int(event.get("number_of_events", 5))
    successful = 0

    for _ in range(number_of_events):
        mock_event = generate_event()
        try:
            response = kinesis.put_record(
                StreamName=STREAM_NAME,
                Data=json.dumps(mock_event).encode("utf-8"),
                PartitionKey=str(mock_event["id_municipio"])
            )
            successful += 1
        except Exception as e:
            print(f"Failed to send event: {mock_event}. Error: {e}")

        finally:
            time.sleep(random.choice([0, 1, 2, 3]))

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Eventos enviados para o Kinesis",
            "requested": number_of_events,
            "successful": successful,
            "failed": number_of_events - successful
        })
    }