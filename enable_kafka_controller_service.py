import json
import uuid
import requests


def enable_kafka_controller_service(ip_address, port, kafka_client_uuidv4, kafka_controller_service_id):
    nifi_api_endpoint = f'http://{ip_address}:{port}/nifi-api'

    print(f'Enable the Kafka Controller Service: {kafka_client_uuidv4}')

    headers = {
        'Content-Type': 'application/json',
    }
    dicts = {
        'revision': {
            'clientId': kafka_client_uuidv4,
            'version': 2,
        },
        'disconnectedNodeAcknowledged': False,
        'state': 'ENABLED',
        'uiOnly': True,
    }
    response = requests.put(f'{nifi_api_endpoint}/controller-services/{kafka_controller_service_id}/run-status', headers=headers, data=json.dumps(dicts))

    if response.ok is False:
        print(response.status_code)
        print(response.text)
        exit(1)


    print('Enable the KafkaConnection Controller Service is done.\n')
