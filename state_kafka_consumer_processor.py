import json
import uuid
import requests


def state_kafka_consumer_processor(ip_address, port, client_uuidv4, processor_id, version=1, state='RUNNING'):
    nifi_api_endpoint = f'http://{ip_address}:{port}/nifi-api'

    print(f'Let the ConsumeKafka processor state be {state}')

    client_uuidv4 = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json',
    }
    dicts = {
        'revision': {
            'clientId': client_uuidv4,
            'version': version,
        },
        'disconnectedNodeAcknowledged': False,
        'state': state,
    }
    response = requests.put(f'{nifi_api_endpoint}/processors/{processor_id}/run-status', headers=headers, data=json.dumps(dicts))

    if response.ok is False:
        print(response.status_code)
        print(response.text)
        exit(1)


    print('The ConsumeKafka processor state is updated.\n')
