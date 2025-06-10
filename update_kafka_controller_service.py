import json
import uuid
import requests


def update_kafka_controller_service(ip_address, port, kafka_client_uuidv4, kafka_controller_service_id, kafka_broker_server):
    nifi_api_endpoint = f'http://{ip_address}:{port}/nifi-api'

    print(f'Update the Kafka Controller Service about Kafka broker address')

    headers = {
        'Content-Type': 'application/json',
    }
    dicts = {
        'revision': {
            'clientId': kafka_client_uuidv4,
            'version': 1,
        },
        'disconnectedNodeAcknowledged': False,
        'component': {
            'id': kafka_controller_service_id,
            'name': 'Kafka3ConnectionService',
            'bulletinLevel': 'WARN',
            'comments': '',
            'properties': {
                'bootstrap.servers': kafka_broker_server,
            },
            'sensitiveDynamicPropertyNames': [],
        },
    }
    response = requests.put(f'{nifi_api_endpoint}/controller-services/{kafka_controller_service_id}', headers=headers, data=json.dumps(dicts))

    if response.ok is False:
        print(response.status_code)
        print(response.text)
        exit(1)

    print('Update the KafkaConnection Controller Service about Kafka broker address is done.\n')
