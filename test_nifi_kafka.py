import time
import state_kafka_consumer_processor
import update_kafka_consumer_processor


ip_address = '127.0.0.1'
port = 28080
nifi_version = '2.0.0'

kafka_consumer_processor_client_id = '1fb9aafe-dd09-44a8-b162-42fca6a132c9'
kafka_consumer_processor_id = '590d56b5-0197-1000-0000-000078fdbf0a'
kafka_controller_service_id = '590d561a-0197-1000-0000-000031044f07'

version = 24

print('Let ConsumeKafka Processor state be RUNNING')
state_kafka_consumer_processor.state_kafka_consumer_processor(
    ip_address,
    port,
    kafka_consumer_processor_client_id,
    kafka_consumer_processor_id,
    version,
    'RUNNING'
)

seconds = 180
print(f'Sleep {seconds} to run the ConsumeKafka Processor')
time.sleep(180)

version += 1

kafka_group_id = 'my-group-id'
kafka_topic_name = 'kafka_nifi_topic'
print(f'Update ConsumeKafka Processor topic is {kafka_topic_name}')


print('Let ConsumeKafka Processor state be STOPPED')
state_kafka_consumer_processor.state_kafka_consumer_processor(
    ip_address,
    port,
    kafka_consumer_processor_client_id,
    kafka_consumer_processor_id,
    version,
    'STOPPED'
)

seconds = 15
print(f'Waiting {seconds} to let the ConsumeKafka Processor state is stopped.')
time.sleep(seconds)

version += 1
update_kafka_consumer_processor.update_kafka_consumer_processor(
    ip_address,
    port,
    kafka_consumer_processor_client_id,
    kafka_consumer_processor_id,
    kafka_controller_service_id,
    kafka_group_id,
    kafka_topic_name,
    version
)

version += 1

print('Let ConsumeKafka Processor state be RUNNING')
state_kafka_consumer_processor = state_kafka_consumer_processor.state_kafka_consumer_processor(
    ip_address,
    port,
    kafka_consumer_processor_client_id,
    kafka_consumer_processor_id,
    version,
    'RUNNING'
)
