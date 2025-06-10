import json
import uuid
import requests


def create_kafka_controller_service(ip_address, port, process_group_id, nifi_version='2.0.0'):
    nifi_api_endpoint = f'http://{ip_address}:{port}/nifi-api'

    print(f'Create the Kafka Controller Service')

    kafka_client_uuidv4 = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json',
    }
    dicts = {
        'revision': {
            'clientId': kafka_client_uuidv4,
            'version': 0,
        },
        'disconnectedNodeAcknowledged': False,
        'component': {
            'bundle': {
                'group': 'org.apache.nifi',
                'artifact': 'nifi-kafka-3-service-nar',
                'version': nifi_version,
            },
            'type': 'org.apache.nifi.kafka.service.Kafka3ConnectionService',
        },
    }
    response = requests.post(f'{nifi_api_endpoint}/process-groups/{process_group_id}/controller-services', headers=headers, data=json.dumps(dicts))

    if response.ok is False:
        print(response.status_code)
        print(response.text)
        exit(1)


    controller_service_id = response.json()['id']
    print('The KafkaConnection Controller Service is created.')
    print(f'The KafkaConnection Controller Service id is {controller_service_id}')
    print(f'The KafkaConnection Controller Service Client id is {kafka_client_uuidv4}')
    print('\n')

    return {
        'kafka_client_id': kafka_client_uuidv4,
        'kafka_controller_service_id': controller_service_id,
    }
