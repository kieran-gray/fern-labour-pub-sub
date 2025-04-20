import asyncio
import json
import logging
from concurrent.futures import Future
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, Mock, call, patch

import pytest
from dishka import AsyncContainer
from google.api_core import exceptions as api_exceptions
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message

from gcp_pub_sub_dishka.event_handler import EventHandler
from gcp_pub_sub_dishka.event import Event
from gcp_pub_sub_dishka.consumer import PubSubEventConsumer


MODULE_PATH = "gcp_pub_sub_dishka.consumer"
TEST_PROJECT_ID = "test-project"


class MockPlaceHolderHandler(EventHandler):
    async def handle(self, data: dict) -> None:
        pass


@pytest.fixture
def mock_subscriber_client():
    """Fixture for mocking SubscriberClient."""
    client = MagicMock(spec=pubsub_v1.SubscriberClient)
    client.subscription_path = MagicMock(
        side_effect=lambda project, sub: f"projects/{project}/subscriptions/{sub}"
    )
    client.subscribe = Mock()
    client.close = Mock()
    return client


@pytest.fixture
def mock_streaming_pull_future():
    """Fixture for mocking StreamingPullFuture."""
    future = MagicMock(spec=StreamingPullFuture)
    future.result = Mock()
    future.cancel = Mock()
    future.running = MagicMock(return_value=True)
    future.add_done_callback = MagicMock()
    return future


@pytest.fixture
def mock_async_container():
    """Fixture for mocking dishka AsyncContainer."""
    request_container_mock = AsyncMock(spec=AsyncContainer)
    request_container_mock.get = AsyncMock()

    mock_context = AsyncMock()
    mock_context.__aenter__.return_value = request_container_mock
    mock_context.__aexit__.return_value = AsyncMock(return_value=None)

    container = MagicMock(spec=AsyncContainer)
    container.__call__ = MagicMock(return_value=mock_context)
    return container


@pytest.fixture
def mock_message():
    """Fixture for mocking Pub/Sub Message."""
    event = Event(
        id="evt-123",
        type="labour.begun",
        data={"key": "value"},
        time=datetime(2020, 1, 1, 12),
    )
    message = MagicMock(spec=Message)
    message.ack = Mock()
    message.nack = Mock()
    message.data = json.dumps(event.to_dict()).encode("utf-8")
    message.attributes = {"attribute_key": "attribute_value"}
    message.ack_id = f"projects/{TEST_PROJECT_ID}/subscriptions/labour.begun.sub:#MSG123"
    message.message_id = "test-message-id-123"
    return message


@pytest.fixture
def consumer_handlers():
    """Define handlers mapping for tests."""

    return {
        "labour.begun": MockPlaceHolderHandler,
        "labour.completed": MockPlaceHolderHandler,
    }


@pytest.fixture
def consumer(mock_subscriber_client, consumer_handlers):
    """Fixture for PubSubEventConsumer."""
    return PubSubEventConsumer(
        project_id=TEST_PROJECT_ID,
        subscriber=mock_subscriber_client,
        topic_handlers=consumer_handlers,
    )


def test_consumer_initialization(mock_subscriber_client, consumer_handlers):
    """Test consumer initializes correctly."""
    consumer = PubSubEventConsumer(
        project_id=TEST_PROJECT_ID,
        subscriber=mock_subscriber_client,
        topic_handlers=consumer_handlers,
    )
    assert consumer._subscriber == mock_subscriber_client
    assert consumer._project_id == TEST_PROJECT_ID


@patch(f"{MODULE_PATH}.pubsub_v1.SubscriberClient", autospec=True)
def test_consumer_initialization_creates_client(MockSubscriberClient, consumer_handlers):
    """Test consumer creates a client if none provided."""
    consumer = PubSubEventConsumer(project_id=TEST_PROJECT_ID, topic_handlers=consumer_handlers)
    MockSubscriberClient.assert_called_once()
    assert consumer._subscriber == MockSubscriberClient.return_value


def test_set_container(consumer: PubSubEventConsumer, mock_async_container):
    """Test setting the DI container."""
    consumer.set_container(mock_async_container)
    assert consumer._container == mock_async_container


def test_get_subscription_path(consumer: PubSubEventConsumer):
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
    mock_async_container: MagicMock,
    mock_message: MagicMock,
    caplog,
):
    """Test processing message when no handler matches."""
    event = Event(
        id="evt-123",
        type="not-found.topic",
        data={"key": "value"},
        time=datetime(2020, 1, 1, 12),
    )
    mock_message.data = json.dumps(event.to_dict()).encode("utf-8")
    consumer.set_container(mock_async_container)

    with caplog.at_level(logging.ERROR):
        await consumer._process_message(mock_message)

    assert "No handler found for message" in caplog.text
    mock_message.nack.assert_called_once()
    mock_message.ack.assert_not_called()


async def test_process_message_json_decode_error(
    consumer: PubSubEventConsumer,
    mock_async_container: MagicMock,
    mock_message: MagicMock,
    caplog,
):
    """Test processing message with invalid JSON data."""
    mock_message.data = b'{"invalid json'
    mock_message.ack_id = f"projects/{TEST_PROJECT_ID}/subscriptions/labour.begun.sub:#MSG123"
    consumer.set_container(mock_async_container)

    with caplog.at_level(logging.ERROR):
        await consumer._process_message(mock_message)

    assert "Failed to decode JSON message data" in caplog.text
    mock_message.nack.assert_called_once()
    mock_message.ack.assert_not_called()


async def test_process_message_no_container(
    consumer: PubSubEventConsumer, mock_message: MagicMock, caplog
):
    """Test processing message when DI container is not set."""
    with caplog.at_level(logging.ERROR):
        await consumer._process_message(mock_message)
    assert "Dependency injection container not set" in caplog.text
    mock_message.nack.assert_called_once()


@patch(f"{MODULE_PATH}.asyncio.run_coroutine_threadsafe", autospec=True)
def test_message_callback_success(
    mock_run_coro: MagicMock,
    consumer: PubSubEventConsumer,
    mock_message: MagicMock,
):
    """Test the sync callback correctly schedules async processing."""
    mock_loop = MagicMock(spec=asyncio.AbstractEventLoop)
    consumer._loop = mock_loop
    consumer._running = True
    mock_future = MagicMock(spec=Future)
    mock_run_coro.return_value = mock_future

    consumer._message_callback(mock_message)

    assert mock_run_coro.call_count == 1
    call_args = mock_run_coro.call_args[0]
    assert call_args[1] == mock_loop
    assert asyncio.iscoroutine(call_args[0])

    assert mock_future.add_done_callback.call_count == 1


def test_message_callback_not_running(
    consumer: PubSubEventConsumer, mock_message: MagicMock, caplog
):
    """Test callback NACKs if consumer is not running."""
    consumer._running = False
    with caplog.at_level(logging.WARNING):
        consumer._message_callback(mock_message)
    assert "Consumer not running, NACKing message" in caplog.text
    mock_message.nack.assert_called_once()


def test_message_callback_no_loop(consumer: PubSubEventConsumer, mock_message: MagicMock, caplog):
    """Test callback NACKs if loop is not set."""
    consumer._running = True
    consumer._loop = None
    with caplog.at_level(logging.ERROR):
        consumer._message_callback(mock_message)
    assert "Event loop not available in callback" in caplog.text
    mock_message.nack.assert_called_once()


def test_on_future_done_success(consumer: PubSubEventConsumer, mock_streaming_pull_future, caplog):
    """Test done callback when future completes normally."""
    sub_path = "projects/test/subscriptions/labour.begun.sub"
    consumer._streaming_pull_futures[sub_path] = mock_streaming_pull_future
    mock_streaming_pull_future.result.return_value = None

    with caplog.at_level(logging.INFO):
        consumer._on_future_done(sub_path, mock_streaming_pull_future)

    mock_streaming_pull_future.result.assert_called_once_with(timeout=0)
    assert f"Streaming pull future for {sub_path} completed normally" in caplog.text
    assert sub_path not in consumer._streaming_pull_futures


def test_on_future_done_error(consumer: PubSubEventConsumer, mock_streaming_pull_future, caplog):
    """Test done callback when future fails."""
    sub_path = "projects/test/subscriptions/labour.begun.sub"
    consumer._streaming_pull_futures[sub_path] = mock_streaming_pull_future
    test_exception = api_exceptions.NotFound("Subscription gone")
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
    mock_async_container: MagicMock,
):
    """Test starting the consumer successfully."""
    consumer.set_container(mock_async_container)
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


async def test_start_no_handlers(consumer: PubSubEventConsumer, caplog):
    """Test start fails and logs error if no handlers registered."""
    consumer._handlers = {}
    consumer.set_container(MagicMock(spec=AsyncContainer))

    with caplog.at_level(logging.ERROR):
        await consumer.start()

    assert consumer._running is False
    assert "No event handlers registered." in caplog.text


async def test_start_no_container(consumer: PubSubEventConsumer, caplog):
    """Test start fails and logs error if DI container not set."""
    with caplog.at_level(logging.ERROR):
        await consumer.start()
    assert consumer._running is False
    assert "Dependency injection container not set." in caplog.text


async def test_stop(
    consumer: PubSubEventConsumer,
    mock_subscriber_client: MagicMock,
    mock_streaming_pull_future: MagicMock,
):
    """Test stopping the consumer."""
    sub_path = "projects/test/subscriptions/labour.begun.sub"
    consumer._running = True
    consumer._streaming_pull_futures[sub_path] = mock_streaming_pull_future
    consumer._loop = asyncio.get_running_loop()

    async def mock_executor_close(_, func, *args):
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


async def test_is_healthy(consumer: PubSubEventConsumer, mock_streaming_pull_future):
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
