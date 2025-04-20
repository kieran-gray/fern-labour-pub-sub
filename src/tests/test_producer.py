import asyncio
import json
import logging
from concurrent.futures import TimeoutError
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.futures import Future as PubSubFuture

# Assuming these exist or create simple placeholders for tests
from gcp_pub_sub_dishka.event import Event
from gcp_pub_sub_dishka.producer import FUTURE_TIMEOUT_SECONDS, PubSubEventProducer

MODULE_PATH = "gcp_pub_sub_dishka.producer"
TEST_PROJECT_ID = "test-project"


@pytest.fixture
def mock_publisher_client():
    """Fixture for mocking PublisherClient."""
    mock_client = MagicMock(spec=pubsub_v1.PublisherClient)
    # Mock the topic_path method
    mock_client.topic_path = MagicMock(
        side_effect=lambda project, topic: f"projects/{project}/topics/{topic}"
    )
    mock_future = MagicMock(spec=PubSubFuture)
    mock_future.result = Mock(return_value="default-mock-message-id")
    mock_client.publish.return_value = mock_future
    return mock_client


@pytest.fixture
def mock_pubsub_future():
    """Fixture for mocking PubSubFuture."""
    mock_future = MagicMock(spec=PubSubFuture)
    mock_future.result = Mock(return_value="mock-message-id")
    mock_future.cancel = Mock()
    return mock_future


@pytest.fixture
def producer(mock_publisher_client):
    """Fixture for PubSubEventProducer with mocked client."""
    return PubSubEventProducer(project_id=TEST_PROJECT_ID, publisher=mock_publisher_client)


@pytest.fixture
def sample_event():
    """Fixture for a sample Event."""
    return Event(
        id="evt-123",
        type="sample.event-happened",
        data={"key": "value"},
        time=datetime(2020, 1, 1, 12),
    )


def test_producer_initialization(mock_publisher_client):
    """Test producer initializes correctly with a provided client."""
    producer = PubSubEventProducer(project_id=TEST_PROJECT_ID, publisher=mock_publisher_client)
    assert producer._publisher == mock_publisher_client
    assert producer._project_id == TEST_PROJECT_ID


@patch(f"{MODULE_PATH}.pubsub_v1.PublisherClient", autospec=True)
def test_producer_initialization_creates_client(MockPublisherClient):
    """Test producer initializes correctly and creates a client if none provided."""
    producer = PubSubEventProducer(project_id=TEST_PROJECT_ID)
    assert producer._project_id == TEST_PROJECT_ID
    MockPublisherClient.assert_called_once()
    assert producer._publisher == MockPublisherClient.return_value


def test_get_topic_path(producer: PubSubEventProducer, sample_event: Event):
    """Test _get_topic_path generates the correct path."""
    expected_topic_id = "sample.event-happened"  # Lowercased, underscores to hyphens
    expected_path = f"projects/{TEST_PROJECT_ID}/topics/{expected_topic_id}"
    actual_path = producer._get_topic_path(sample_event)
    assert actual_path == expected_path
    producer._publisher.topic_path.assert_called_once_with(TEST_PROJECT_ID, expected_topic_id)


def test_serialize_event(producer: PubSubEventProducer, sample_event: Event):
    """Test _serialize_event correctly serializes the event."""
    expected_dict = {
        "id": "evt-123",
        "type": "sample.event-happened",
        "data": {"key": "value"},
        "time": "2020-01-01T12:00:00",
    }
    expected_bytes = json.dumps(expected_dict).encode("utf-8")
    actual_bytes = producer._serialize_event(sample_event)
    assert actual_bytes == expected_bytes


async def test_publish_success(
    producer: PubSubEventProducer,
    mock_publisher_client: MagicMock,
    mock_pubsub_future: MagicMock,
    sample_event: Event,
    caplog,
):
    """Test successful publish of a single event."""
    mock_publisher_client.publish.return_value = mock_pubsub_future
    topic_path = producer._get_topic_path(sample_event)
    event_data_bytes = producer._serialize_event(sample_event)
    attributes = {"event_id": str(sample_event.id)}

    with patch("asyncio.get_running_loop") as mock_loop:
        mock_loop.return_value.run_in_executor = AsyncMock(return_value="mock-message-id")

        with caplog.at_level(logging.DEBUG):
            await producer.publish(sample_event)

        mock_publisher_client.publish.assert_called_once_with(
            topic_path, data=event_data_bytes, **attributes
        )
        mock_loop.return_value.run_in_executor.assert_called_once_with(
            None, mock_pubsub_future.result, FUTURE_TIMEOUT_SECONDS
        )
        assert (
            f"Published event {sample_event.id} to {topic_path} with message ID mock-message-id"
            in caplog.text
        )


async def test_publish_timeout(
    producer: PubSubEventProducer,
    mock_publisher_client: MagicMock,
    mock_pubsub_future: MagicMock,
    sample_event: Event,
    caplog,
):
    """Test publish timeout scenario."""
    mock_publisher_client.publish.return_value = mock_pubsub_future

    async def mock_executor_timeout(*_, **__):
        raise TimeoutError("Simulated executor timeout")

    with patch("asyncio.get_running_loop") as mock_loop:
        mock_loop.return_value.run_in_executor = mock_executor_timeout

        with caplog.at_level(logging.CRITICAL):
            await producer.publish(sample_event)

    assert f"Timeout error while publishing event {sample_event.id}" in caplog.text


async def test_publish_general_exception(
    producer: PubSubEventProducer,
    mock_publisher_client: MagicMock,
    mock_pubsub_future: MagicMock,
    sample_event: Event,
    caplog,
):
    """Test publish with a general exception during result retrieval."""
    mock_publisher_client.publish.return_value = mock_pubsub_future
    test_exception = ValueError("Something went wrong")

    async def mock_executor_value_error(*_, **__):
        raise test_exception

    with patch("asyncio.get_running_loop") as mock_loop:
        mock_loop.return_value.run_in_executor = mock_executor_value_error

        with caplog.at_level(logging.CRITICAL):
            await producer.publish(sample_event)

    assert f"Unexpected error while publishing event {sample_event.id}" in caplog.text


async def test_publish_batch_success(
    producer: PubSubEventProducer,
    mock_publisher_client: MagicMock,
    sample_event: Event,
    caplog,
):
    """Test successful publish of a batch of events."""
    event1 = sample_event
    event2 = Event(
        id="evt-456", type="Other.Event", data={"more": "data"}, time=datetime.now(UTC)
    )
    events = [event1, event2]

    future1 = MagicMock(spec=PubSubFuture)
    future1.result = MagicMock(return_value="msg-id-1")
    future2 = MagicMock(spec=PubSubFuture)
    future2.result = MagicMock(return_value="msg-id-2")
    mock_publisher_client.publish.side_effect = [future1, future2]

    async def mock_executor(_, func, *args):
        return func(*args)

    with patch("asyncio.get_running_loop") as mock_loop:
        mock_loop.return_value.run_in_executor = mock_executor
        with caplog.at_level(logging.INFO):
            await producer.publish_batch(events)

        assert mock_publisher_client.publish.call_count == 2
        assert "Successfully published 2 out of 2 events in the batch." in caplog.text


async def test_publish_batch_partial_failure(
    producer: PubSubEventProducer,
    mock_publisher_client: MagicMock,
    sample_event: Event,
    caplog,
):
    """Test batch publish with one success and one timeout."""
    event1 = sample_event  # Will succeed
    event2 = Event(
        id="evt-456", type="Other.Event", data={"more": "data"}, time=datetime.now(UTC)
    )
    events = [event1, event2]

    future1 = MagicMock(spec=PubSubFuture)
    future1.result = MagicMock(return_value="msg-id-1")
    future2 = MagicMock(spec=PubSubFuture)
    future2.result = MagicMock(side_effect=TimeoutError("Timeout publishing evt-456"))
    mock_publisher_client.publish.side_effect = [future1, future2]

    async def mock_executor_gather_safe(_, func, *args):
        await asyncio.sleep(0.01)
        try:
            return func(*args)
        except Exception as e:
            return e

    with patch("asyncio.get_running_loop") as mock_loop:
        mock_loop.return_value.run_in_executor = mock_executor_gather_safe
        with caplog.at_level(logging.INFO):
            await producer.publish_batch(events)

        assert mock_publisher_client.publish.call_count == 2
        assert "Successfully published 1 out of 2 events in the batch." in caplog.text
        assert "Failed to publish 1 out of 2 events in the batch." in caplog.text
        assert "Timed out publishing event evt-456" in caplog.text


async def test_publish_batch_empty(producer: PubSubEventProducer, caplog):
    """Test publishing an empty batch."""
    with caplog.at_level(logging.DEBUG):
        await producer.publish_batch([])
        assert "No events to publish in batch." in caplog.text


async def test_publish_batch_error_during_dispatch(
    producer: PubSubEventProducer,
    mock_publisher_client: MagicMock,
    sample_event: Event,
    caplog,
):
    """Test batch publish where an error occurs during the initial publish call
    for one event, but the other succeeds."""
    event1 = sample_event  # Will fail during dispatch
    event2 = Event(
        id="evt-456", type="Other.Event", data={"more": "data"}, time=datetime(2020, 1, 1, 12)
    )  # Will succeed
    events = [event1, event2]

    mock_future_2 = MagicMock(spec=PubSubFuture)
    mock_future_2.result = MagicMock(return_value="msg-id-2")  # Simulate success

    mock_publisher_client.publish.side_effect = [ValueError("Setup failed"), mock_future_2]

    async def mock_executor_gather_safe(_, func, *args):
        await asyncio.sleep(0.01)
        return func(*args)

    with patch("asyncio.get_running_loop") as mock_loop:
        mock_loop.return_value.run_in_executor = mock_executor_gather_safe
        with caplog.at_level(logging.DEBUG):
            await producer.publish_batch(events)

    assert f"Failed to initiate publishing for event {event1.id}: Setup failed" in caplog.text
    assert (
        f"Successfully published event {event2.id} to {producer._get_topic_path(event2)} "
        "with message ID msg-id-2" in caplog.text
    )
    assert "Successfully published 1 out of 2 events in the batch." in caplog.text
    assert "Failed to publish" not in caplog.text.replace(
        f"Failed to initiate publishing for event {event1.id}: Setup failed", ""
    )
