import create_process_group
import create_wait_processor
import connect_kafka_consume_wait
import create_kafka_controller_service
import update_kafka_controller_service
import enable_kafka_controller_service
import create_kafka_consumer_processor
import update_kafka_consumer_processor


ip_address = '127.0.0.1'
port = 28080
nifi_version = '2.0.0'

process_group_result = create_process_group.create_process_group(ip_address, port)
process_group_client_id = process_group_result['process_group_client_id']
process_group_id = process_group_result['process_group_id']

controller_service = create_kafka_controller_service.create_kafka_controller_service(
    ip_address,
    port,
    process_group_id,
    nifi_version
)
kafka_client_id = controller_service['kafka_client_id']
kafka_controller_service_id = controller_service['kafka_controller_service_id']

kafka_broker_server = '140.92.30.171:36716'
update_kafka_controller_service.update_kafka_controller_service(
    ip_address,
    port,
    kafka_client_id,
    kafka_controller_service_id,
    kafka_broker_server
)

enable_kafka_controller_service.enable_kafka_controller_service(
    ip_address,
    port,
    kafka_client_id,
    kafka_controller_service_id
)

create_kafka_consumer_result = create_kafka_consumer_processor.create_kafka_consumer_processor(
    ip_address,
    port,
    process_group_id,
    nifi_version
)

kafka_consumer_processor_client_id = create_kafka_consumer_result['processor_client_id']
kafka_consumer_processor_id = create_kafka_consumer_result['processor_id']

kafka_group_id = 'my-group-id'
kafka_topic_name = 'test_a'
update_kafka_consumer_processor.update_kafka_consumer_processor(
    ip_address,
    port,
    kafka_consumer_processor_client_id,
    kafka_consumer_processor_id,
    kafka_controller_service_id,
    kafka_group_id,
    kafka_topic_name
)

create_wait_processor_result = create_wait_processor.create_wait_processor(
    ip_address,
    port,
    process_group_id,
    nifi_version
)

wait_processor_client_id = create_wait_processor_result['processor_client_id']
wait_processor_id = create_wait_processor_result['processor_id']

connect_kafka_consume_wait.connect_kafka_consume_wait(
    ip_address,
    port,
    process_group_id,
    kafka_consumer_processor_id,
    wait_processor_id
)
