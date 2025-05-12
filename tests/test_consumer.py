import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from typing import Any
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest
import pytest_asyncio
from dishka import AsyncContainer
from fern_labour_core.events.consumer import EventConsumer
from google.api_core import exceptions as api_exceptions
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message
from google.pubsub_v1.types import PullResponse, ReceivedMessage

from fern_labour_pub_sub.consumer import PubSubEventConsumer
from fern_labour_pub_sub.enums import ConsumerMode
from fern_labour_pub_sub.topic_handler import TopicHandler
from tests.conftest import (
    MockDefaultEventHandler,
    MockEvent,
    MockEventHandler,
    MockFailedEventHandler,
)

MODULE_PATH = "fern_labour_pub_sub.consumer"
TEST_PROJECT_ID = "test-project"


@pytest.fixture
def mock_subscriber_client() -> MagicMock:
    """Fixture for mocking SubscriberClient."""
    client = MagicMock(spec=pubsub_v1.SubscriberClient)
    client.subscription_path = MagicMock(
        side_effect=lambda project, sub: f"projects/{project}/subscriptions/{sub}"
    )
    client.subscribe = Mock()
    client.close = Mock()
    return client


@pytest.fixture
def mock_streaming_pull_future() -> MagicMock:
    """Fixture for mocking StreamingPullFuture."""
    future = MagicMock(spec=StreamingPullFuture)
    future.result = Mock()
    future.cancel = Mock()
    future.running = MagicMock(return_value=True)
    future.add_done_callback = MagicMock()
    return future


@pytest.fixture
def mock_message() -> MagicMock:
    """Fixture for mocking Pub/Sub Message."""
    event = MockEvent.create(data={"key": "value"}, event_type="event.begun")
    message = MagicMock(spec=Message)
    message.ack = Mock()
    message.nack = Mock()
    message.data = json.dumps(event.to_dict()).encode("utf-8")
    message.attributes = {"attribute_key": "attribute_value"}
    message.ack_id = f"projects/{TEST_PROJECT_ID}/subscriptions/event.begun.sub:#MSG123"
    message.message_id = "test-message-id-123"
    return message


@pytest.fixture
def mock_received_message(mock_message: MagicMock) -> MagicMock:
    received_message = MagicMock(spec=ReceivedMessage)
    received_message.message = mock_message
    return received_message


@pytest.fixture
def mock_pull_response() -> MagicMock:
    """Fixture for mocking Pub/Sub pull result."""
    response = MagicMock(spec=PullResponse)
    response.received_messages = []
    return response


@pytest.fixture
def consumer_handlers() -> list[TopicHandler]:
    """Define handlers mapping for tests."""
    return [
        TopicHandler(
            topic="event.begun", event_handler=MockEventHandler, component="event_handlers"
        ),
        TopicHandler(
            topic="event.completed",
            event_handler=MockFailedEventHandler,
            component="event_handlers",
        ),
        TopicHandler(topic="event.default", event_handler=MockDefaultEventHandler),
    ]


@pytest_asyncio.fixture
async def consumer(
    mock_subscriber_client: MagicMock, consumer_handlers: list[TopicHandler]
) -> AsyncGenerator[PubSubEventConsumer]:
    """Fixture for PubSubEventConsumer."""
    consumer = PubSubEventConsumer(
        project_id=TEST_PROJECT_ID,
        subscriber=mock_subscriber_client,
        topic_handlers=consumer_handlers,
    )
    yield consumer
    await consumer.stop()


def test_implementation_is_subclass() -> None:
    assert issubclass(PubSubEventConsumer, EventConsumer)


def test_consumer_initialization(
    mock_subscriber_client: MagicMock, consumer_handlers: list[TopicHandler]
) -> None:
    """Test consumer initializes correctly."""
    consumer = PubSubEventConsumer(
        project_id=TEST_PROJECT_ID,
        subscriber=mock_subscriber_client,
        topic_handlers=consumer_handlers,
    )
    assert consumer._subscriber == mock_subscriber_client
    assert consumer._project_id == TEST_PROJECT_ID


@patch(f"{MODULE_PATH}.pubsub_v1.SubscriberClient", autospec=True)
def test_consumer_initialization_creates_client(
    MockSubscriberClient: MagicMock, consumer_handlers: list[TopicHandler]
) -> None:
    """Test consumer creates a client if none provided."""
    consumer = PubSubEventConsumer(project_id=TEST_PROJECT_ID, topic_handlers=consumer_handlers)
    MockSubscriberClient.assert_called_once()
    assert consumer._subscriber == MockSubscriberClient.return_value


def test_set_container(consumer: PubSubEventConsumer, container: AsyncContainer) -> None:
    """Test setting the DI container."""
    consumer.set_container(container)
    assert consumer._container == container


def test_get_subscription_path(consumer: PubSubEventConsumer) -> None:
    """Test _get_subscription_path generates correct path using convention."""
    topic_name = "some.topic.name"
    expected_subscription_id = f"{topic_name}.sub"
    expected_path = f"projects/{TEST_PROJECT_ID}/subscriptions/{expected_subscription_id}"
    actual_path = consumer._get_subscription_path(topic_name)
    assert actual_path == expected_path
    consumer._subscriber.subscription_path.assert_called_once_with(
        TEST_PROJECT_ID, expected_subscription_id
    )


async def test_process_message_no_handler(
    consumer: PubSubEventConsumer,
    container: AsyncContainer,
    mock_message: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test processing message when no handler matches."""
    event = MockEvent.create(data={"key": "value"}, event_type="not-found.topic")
    mock_message.data = json.dumps(event.to_dict()).encode("utf-8")
    consumer.set_container(container)

    with caplog.at_level(logging.ERROR):
        await consumer._process_message(mock_message)

    assert "No handler found for message" in caplog.text
    mock_message.nack.assert_called_once()
    mock_message.ack.assert_not_called()


async def test_process_message_json_decode_error(
    consumer: PubSubEventConsumer,
    container: AsyncContainer,
    mock_message: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test processing message with invalid JSON data."""
    mock_message.data = b'{"invalid json'
    mock_message.ack_id = f"projects/{TEST_PROJECT_ID}/subscriptions/labour.begun.sub:#MSG123"
    consumer.set_container(container)

    with caplog.at_level(logging.ERROR):
        await consumer._process_message(mock_message)

    assert "Failed to decode JSON message data" in caplog.text
    mock_message.nack.assert_called_once()
    mock_message.ack.assert_not_called()


async def test_process_message_no_container(
    consumer: PubSubEventConsumer, mock_message: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Test processing message when DI container is not set."""
    with caplog.at_level(logging.ERROR):
        await consumer._process_message(mock_message)
    assert "Dependency injection container not set" in caplog.text
    mock_message.nack.assert_called_once()


async def test_message_callback_success(
    consumer: PubSubEventConsumer,
    mock_message: MagicMock,
) -> None:
    """Test the sync callback correctly schedules async processing."""
    consumer._loop = asyncio.get_event_loop()
    consumer._running = True

    consumer._message_callback(mock_message)


def test_message_callback_not_running(
    consumer: PubSubEventConsumer, mock_message: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Test callback NACKs if consumer is not running."""
    consumer._running = False
    with caplog.at_level(logging.WARNING):
        consumer._message_callback(mock_message)
    assert "Consumer not running, NACKing message" in caplog.text
    mock_message.nack.assert_called_once()


def test_message_callback_no_loop(
    consumer: PubSubEventConsumer, mock_message: MagicMock, caplog: pytest.LogCaptureFixture
) -> None:
    """Test callback NACKs if loop is not set."""
    consumer._running = True
    consumer._loop = None
    with caplog.at_level(logging.ERROR):
        consumer._message_callback(mock_message)
    assert "Event loop not available in callback" in caplog.text
    mock_message.nack.assert_called_once()


def test_on_future_done_success(
    consumer: PubSubEventConsumer,
    mock_streaming_pull_future: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test done callback when future completes normally."""
    sub_path = "projects/test/subscriptions/labour.begun.sub"
    consumer._streaming_pull_futures[sub_path] = mock_streaming_pull_future
    mock_streaming_pull_future.result.return_value = None

    with caplog.at_level(logging.INFO):
        consumer._on_future_done(sub_path, mock_streaming_pull_future)

    mock_streaming_pull_future.result.assert_called_once_with(timeout=0)
    assert f"Streaming pull future for {sub_path} completed normally" in caplog.text
    assert sub_path not in consumer._streaming_pull_futures


def test_on_future_done_error(
    consumer: PubSubEventConsumer,
    mock_streaming_pull_future: MagicMock,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Test done callback when future fails."""
    sub_path = "projects/test/subscriptions/labour.begun.sub"
    consumer._streaming_pull_futures[sub_path] = mock_streaming_pull_future
    test_exception = api_exceptions.NotFound("Subscription gone")  # type: ignore
    mock_streaming_pull_future.result.side_effect = test_exception

    with caplog.at_level(logging.ERROR):
        consumer._on_future_done(sub_path, mock_streaming_pull_future)

    mock_streaming_pull_future.result.assert_called_once_with(timeout=0)
    assert f"Streaming pull future for {sub_path} failed!" in caplog.text
    assert sub_path not in consumer._streaming_pull_futures


@patch(f"{MODULE_PATH}.asyncio.sleep", new_callable=AsyncMock)
async def test_start_success(
    mock_sleep: AsyncMock,
    consumer: PubSubEventConsumer,
    mock_subscriber_client: MagicMock,
    mock_streaming_pull_future: MagicMock,
    container: AsyncContainer,
) -> None:
    """Test starting the consumer successfully."""
    consumer.set_container(container)
    mock_subscriber_client.subscribe.return_value = mock_streaming_pull_future
    mock_sleep.side_effect = asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await consumer.start()

    assert consumer._running is True
    assert consumer._loop is not None

    expected_sub_calls = []
    for sub_name in consumer._handlers.keys():
        sub_path = f"projects/{TEST_PROJECT_ID}/subscriptions/{sub_name}"
        expected_sub_calls.append(call(subscription=sub_path, callback=consumer._message_callback))

    mock_subscriber_client.subscribe.assert_has_calls(expected_sub_calls, any_order=True)
    assert mock_subscriber_client.subscribe.call_count == len(consumer._handlers)
    assert mock_streaming_pull_future.add_done_callback.call_count == len(consumer._handlers)


async def test_start_no_handlers(
    consumer: PubSubEventConsumer, container: AsyncContainer, caplog: pytest.LogCaptureFixture
) -> None:
    """Test start fails and logs error if no handlers registered."""
    consumer._handlers = {}
    consumer.set_container(container=container)

    with caplog.at_level(logging.ERROR):
        await consumer.start()

    assert consumer._running is False
    assert "No event handlers registered." in caplog.text


async def test_start_already_running(
    consumer: PubSubEventConsumer, caplog: pytest.LogCaptureFixture
) -> None:
    """Test start fails and logs error if consumer already running."""
    consumer._running = True
    with caplog.at_level(logging.WARNING):
        await consumer.start()
    assert "Consumer is already running." in caplog.text


async def test_start_no_container(
    consumer: PubSubEventConsumer, caplog: pytest.LogCaptureFixture
) -> None:
    """Test start fails and logs error if DI container not set."""
    assert consumer._container is None
    with caplog.at_level(logging.ERROR):
        await consumer.start()
    assert consumer._running is False
    assert "Dependency injection container not set." in caplog.text


async def test_stop(
    consumer: PubSubEventConsumer,
    mock_subscriber_client: MagicMock,
    mock_streaming_pull_future: MagicMock,
) -> None:
    """Test stopping the consumer."""
    sub_path = "projects/test/subscriptions/labour.begun.sub"
    consumer._running = True
    consumer._streaming_pull_futures[sub_path] = mock_streaming_pull_future
    consumer._loop = asyncio.get_running_loop()

    async def mock_executor_close(_: Any, func: Any, *args: Any) -> None:
        func(*args)
        return None

    with patch("asyncio.get_running_loop") as mock_loop_getter:
        mock_loop_instance = MagicMock()
        mock_loop_instance.run_in_executor = mock_executor_close
        mock_loop_getter.return_value = mock_loop_instance

        await consumer.stop()

    assert consumer._running is False
    mock_streaming_pull_future.cancel.assert_called_once()
    mock_subscriber_client.close.assert_called_once()
    assert not consumer._streaming_pull_futures


async def test_is_healthy(
    consumer: PubSubEventConsumer, mock_streaming_pull_future: MagicMock
) -> None:
    """Test health check scenarios."""
    consumer._running = False
    assert await consumer.is_healthy() is False

    consumer._running = True
    consumer._subscriber = None
    assert await consumer.is_healthy() is False
    consumer._subscriber = MagicMock()

    consumer._streaming_pull_futures = {}
    assert await consumer.is_healthy() is False

    sub_path = "projects/test/subscriptions/labour.begun.sub"
    mock_streaming_pull_future.running.return_value = True
    consumer._streaming_pull_futures[sub_path] = mock_streaming_pull_future
    assert await consumer.is_healthy() is True

    mock_streaming_pull_future.running.return_value = False
    consumer._streaming_pull_futures[sub_path] = mock_streaming_pull_future
    assert await consumer.is_healthy() is False


async def test_run_unary_pull_success(
    consumer: PubSubEventConsumer,
    mock_subscriber_client: MagicMock,
    mock_pull_response: MagicMock,
    mock_received_message: MagicMock,
    container: AsyncContainer,
) -> None:
    """Run the consumer in unary pull mode successfully."""
    mock_pull_response.received_messages.append(mock_received_message)

    consumer.set_container(container)
    consumer._mode = ConsumerMode.UNARY_PULL
    mock_subscriber_client.pull.return_value = mock_pull_response

    await consumer.start()

    assert mock_received_message.message.ack.call_count == len(consumer._handlers.keys())


async def test_run_unary_pull_no_messages(
    consumer: PubSubEventConsumer,
    mock_subscriber_client: MagicMock,
    mock_pull_response: MagicMock,
    container: AsyncContainer,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Run the consumer in unary pull mode successfully."""
    consumer.set_container(container)
    consumer._mode = ConsumerMode.UNARY_PULL
    mock_subscriber_client.pull.return_value = mock_pull_response

    with caplog.at_level(logging.INFO):
        await consumer.start()

    for handler in consumer._handlers.values():
        assert f"No messages pulled for subscription {handler.sub}"
