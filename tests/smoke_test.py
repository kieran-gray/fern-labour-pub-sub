from gcp_pub_sub_dishka.producer import PubSubEventProducer
from gcp_pub_sub_dishka.consumer import PubSubEventConsumer


def can_instantiate_producer():
    producer = PubSubEventProducer(project_id="test")
    if not producer:
        raise RuntimeError("Producer instantiation failed.")
    print("Instantiated Producer successfully.")


def can_instantiate_consumer():
    consumer = PubSubEventConsumer(project_id="test", topic_handlers=[])
    if not consumer:
        raise RuntimeError("Consumer instantiation failed.")
    print("Instantiated Consumer successfully.")


if __name__ == "__main__":
    can_instantiate_producer()
    can_instantiate_consumer()
