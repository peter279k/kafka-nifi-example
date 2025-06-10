import json
import uuid
import requests


def connect_kafka_consume_wait(ip_address, port, process_group_id, consume_kafka_processor_id, wait_processor_id):
    nifi_api_endpoint = f'http://{ip_address}:{port}/nifi-api'

    print(f'Connect the ConsumeKafka and Wait processors in the specific process group')

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
            'backPressureDataSizeThreshold': '1 GB',
            'backPressureObjectThreshold': 10000,
            'flowFileExpiration': '0 sec',
            'loadBalanceStrategy': 'DO_NOT_LOAD_BALANCE',
            'name': '',
            'labelIndex': 0,
            'prioritizers': [],
            'selectedRelationships': ['success'],
            'source': {
              'groupId': process_group_id,
              'id': consume_kafka_processor_id,
              'type': 'PROCESSOR',
            },
            'destination':{
              'groupId': process_group_id,
              'id': wait_processor_id,
              'type': 'PROCESSOR'
            },
            'loadBalancePartitionAttribute':'',
            'loadBalanceCompression':'DO_NOT_COMPRESS',
            'bends':[]
        },
    }
    response = requests.post(f'{nifi_api_endpoint}/process-groups/{process_group_id}/connections', headers=headers, data=json.dumps(dicts))

    if response.ok is False:
        print(response.status_code)
        print(response.text)
        exit(1)


    connection_id = response.json()['id']

    print('The ConsumeKafka and Wait processors is connected.')
    print(f'The ConsumeKafka and Wait processors connection id is {connection_id}')
    print(f'The ConsumeKafka and Wait processors connection client id is {client_uuidv4}')
    print('\n')

    return {
        'connection_client_id': client_uuidv4,
        'conneciton_id': connection_id,
    }
