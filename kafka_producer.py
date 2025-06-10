import requests
from kafka import KafkaProducer


print('Requesting the covid-api.com/api')

broker_address = '127.0.0.1:36716'
report_total_url = 'https://covid-api.com/api/reports/total?date=2020-03-14&iso=USA'
region_url = 'https://covid-api.com/api/regions?per_page=20'
headers = {
    'Accept': 'application/json',
    'X-CSRF-TOKEN': '',
}

response = requests.get(report_total_url, headers=headers)
producer = KafkaProducer(bootstrap_servers=broker_address)

# Block until a single message is sent (or timeout)
future = producer.send('test_a', response.text.encode('utf-8'))
result = future.get(timeout=60)
producer.flush()
print(result)

response = requests.get(region_url, headers=headers)
producer = KafkaProducer(bootstrap_servers=broker_address)

# Block until a single message is sent (or timeout)
future = producer.send('kafka_nifi_topic', response.text.encode('utf-8'))
result = future.get(timeout=60)
producer.flush()
print(result)
