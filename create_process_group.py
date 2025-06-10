import json
import uuid
import requests


def create_process_group(ip_address, port):
    nifi_api_endpoint = f'http://{ip_address}:{port}/nifi-api'

    process_group = 'Auto Kafka and NiFi'

    pg_name = 'NiFi Flow'
    pg_keyword = 'process-groups'
    pg_id_string = ''

    print(f'Find the {pg_name} group')

    response = requests.get(f'{nifi_api_endpoint}/resources')
    resp_json = response.json()
    resources = resp_json['resources']
    for resource in resources:
        if resource['name'] == pg_name and pg_keyword in resource['identifier']:
            pg_id_string = resource['identifier'].split('/')[-1]
            break


    print(f'The Process Group id: {pg_id_string} has been found.')
    print(f'Creating the process group: {process_group}')

    client_uuidv4 = str(uuid.uuid4())
    headers = {
        'Content-Type': 'application/json',
    }
    dicts = {
        'revision': {
            'clientId': client_uuidv4,
            'version': 0,
        },
        'component': {
            'name': process_group,
            'position': {
                'x': 300,
                'y': -300,
            },
        },
    }
    response = requests.post(f'{nifi_api_endpoint}/process-groups/{pg_id_string}/process-groups', headers=headers, data=json.dumps(dicts))

    if response.ok is False:
        print(response.status_code)
        print(response.text)
        exit(1)

    process_group_id = response.json()['id']

    print('The Process Group is created.')
    print(f'The Process Group id is {process_group_id}')
    print(f'The Process Group client id is {client_uuidv4}')
    print('\n')

    return {
        'process_group_client_id': client_uuidv4,
        'process_group_id': process_group_id,
    }
