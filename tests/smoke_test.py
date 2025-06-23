from dishka import make_async_container

from fern_labour_pub_sub.consumer import PubSubEventConsumer
from fern_labour_pub_sub.producer import PubSubEventProducer


def can_instantiate_producer() -> None:
    producer = PubSubEventProducer(project_id="test")
    if not producer:
        raise RuntimeError("Producer instantiation failed.")
    print("Instantiated Producer successfully.")


def can_instantiate_consumer() -> None:
    container = make_async_container()

    consumer = PubSubEventConsumer(project_id="test", topic_handlers=[], container=container)
    if not consumer:
        raise RuntimeError("Consumer instantiation failed.")
    print("Instantiated Consumer successfully.")


if __name__ == "__main__":
    can_instantiate_producer()
    can_instantiate_consumer()
