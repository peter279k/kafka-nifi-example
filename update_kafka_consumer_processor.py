import json
import uuid
import requests


def update_kafka_consumer_processor(ip_address, port, client_uuidv4, processor_id, kafka_controller_service_id, kafka_group_id, kafka_topic_name, version=1):
    nifi_api_endpoint = f'http://{ip_address}:{port}/nifi-api'

    print(f'Update the ConsumeKafka processor in the specific process group')

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
        'component': {
            'id': processor_id,
            'name': 'ConsumeKafka',
            'config': {
                'penaltyDuration': '30 sec',
                'yieldDuration': '1 sec',
                'bulletinLevel': 'WARN',
                'schedulingStrategy': 'TIMER_DRIVEN',
                'concurrentlySchedulableTaskCount': 1,
                'schedulingPeriod': '0 sec',
                'executionNode': 'ALL',
                'autoTerminatedRelationships': [],
                'retriedRelationships': [],
                'comments': '',
                'properties': {
                    'Kafka Connection Service': kafka_controller_service_id,
                    'Group ID': kafka_group_id,
                    'Topics': kafka_topic_name,
                },
                'sensitiveDynamicPropertyNames':[],
            },
        },
    }
    response = requests.put(f'{nifi_api_endpoint}/processors/{processor_id}', headers=headers, data=json.dumps(dicts))

    if response.ok is False:
        print(response.status_code)
        print(response.text)
        exit(1)


    print('The ConsumeKafka processor is updated.\n')
