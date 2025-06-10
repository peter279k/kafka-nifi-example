from kafka import KafkaConsumer
from kafka.errors import TopicAlreadyExistsError
from kafka.admin import KafkaAdminClient, NewTopic


def kafka_admin_client():
    ip_address = '127.0.0.1'
    port = 36716
    admin_client = KafkaAdminClient(bootstrap_servers=f'{ip_address}:{port}', security_protocol='PLAINTEXT')
    return admin_client

topic_names = ['kafka_nifi_topic', 'test_a', 'test_b']

def create_topics(topic_names):
    topic_list = []
    for topic in topic_names:
        topic_list.append(NewTopic(name=topic, num_partitions=3, replication_factor=3))
    try:
        admin_client = kafka_admin_client()
        if topic_list:
            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            print("Topic Created Successfully")
        else:
            print("Topic Exist")
    except TopicAlreadyExistsError as e:
        print(f"Topic Already Exist, {e}")
    except Exception as e:
        print(e)


create_topics(topic_names)
