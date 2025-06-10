import json
import uuid
import requests


def create_kafka_consumer_processor(ip_address, port, process_group_id, nifi_version):
    nifi_api_endpoint = f'http://{ip_address}:{port}/nifi-api'

    print(f'Create the ConsumeKafka processor in the specific process group')

    client_uuidv4 = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json',
    }
    dicts = {
        'revision': {
            'clientId': client_uuidv4,
            'version': 0,
        },
        'disconnectedNodeAcknowledged': False,
        'component': {
            'position': {
                'x': -742,
                'y': -533,
            },
            'type': 'org.apache.nifi.kafka.processors.ConsumeKafka',
            'bundle': {
                'group': 'org.apache.nifi',
                'artifact': 'nifi-kafka-nar',
                'version': nifi_version,
            },
        },
    }
    response = requests.post(f'{nifi_api_endpoint}/process-groups/{process_group_id}/processors', headers=headers, data=json.dumps(dicts))

    if response.ok is False:
        print(response.status_code)
        print(response.text)
        exit(1)


    processor_id = response.json()['id']

    print('The ConsumeKafka processor is created.')
    print(f'The ConsumeKafka processor id is {processor_id}')
    print(f'The ConsumeKafka processor client id is {client_uuidv4}')
    print('\n')

    return {
        'processor_client_id': client_uuidv4,
        'processor_id': processor_id,
    }
