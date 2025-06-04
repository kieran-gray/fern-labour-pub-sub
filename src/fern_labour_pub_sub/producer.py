import asyncio
import json
import logging
from collections.abc import Sequence
from concurrent.futures import TimeoutError

from fern_labour_core.events.event import DomainEvent
from fern_labour_core.events.producer import BatchPublishResult
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.publisher.futures import Future

log = logging.getLogger(__name__)


class PubSubEventProducer:
    """
    Google Pub/Sub-based event producer.

    This class handles the publishing of events to Pub/Sub topics,
    supporting both single-event and batch-event publishing.
    """

    def __init__(
        self,
        project_id: str,
        retries: int = 3,
        publisher: pubsub_v1.PublisherClient | None = None,
        future_timeout_seconds: int = 30,
    ):
        """
        Initialize the PubSubEventProducer.

        Args:
            project_id: Google Cloud project ID.
            retries: Number of retries for failed publish operations.
            publisher: Optional custom PublisherClient instance.
        """
        self._publisher = publisher or pubsub_v1.PublisherClient()
        self._project_id = project_id
        self._retries = retries
        self._future_timeout_seconds = future_timeout_seconds

    def _get_topic_path(self, event: DomainEvent) -> str:
        """
        Generate full Pub/Sub topic path based on event type.

        Args:
            event: The event to publish.

        Returns:
            The generated Pub/Sub topic path.
        """
        return str(self._publisher.topic_path(self._project_id, event.type.lower()))

    def _serialize_event(self, event: DomainEvent) -> bytes:
        """
        Serialize event data to bytes for Pub/Sub.

        Args:
            event: The event to serialize.

        Returns:
            Serialized event data as bytes.
        """
        return json.dumps(event.to_dict()).encode("utf-8")

    async def publish(self, event: DomainEvent) -> bool:
        """
        Publish a single event to Pub/Sub.

        Args:
            event: The event to publish.
        """
        topic_path = self._get_topic_path(event)
        data = self._serialize_event(event)

        attributes = {"event_id": event.id}

        try:
            future: Future = self._publisher.publish(topic_path, data=data, **attributes)
            loop = asyncio.get_running_loop()
            message_id = await loop.run_in_executor(
                None, future.result, self._future_timeout_seconds
            )
            log.debug(f"Published event {event.id} to {topic_path} with message ID {message_id}")
            return True
        except TimeoutError as e:
            log.critical(f"Timeout error while publishing event {event.id}", exc_info=e)
        except Exception as e:
            log.critical(f"Unexpected error while publishing event {event.id}", exc_info=e)
        return False

    async def publish_batch(self, events: Sequence[DomainEvent]) -> BatchPublishResult:
        """
        Publish multiple events to Pub/Sub.

        Args:
            events: The sequence of events to publish.
        """
        if not events:
            log.debug("No events to publish in batch.")
            return BatchPublishResult(success_ids=[], failure_ids=[])

        loop = asyncio.get_running_loop()
        publish_tasks = []
        event_details = {}

        for event in events:
            try:
                topic_path = self._get_topic_path(event)
                data = self._serialize_event(event)
                attributes = {"event_id": str(event.id)}

                log.debug(f"Queueing event {event.id} for topic {topic_path}")
                future: Future = self._publisher.publish(topic_path, data=data, **attributes)

                task = loop.run_in_executor(None, future.result, self._future_timeout_seconds)
                publish_tasks.append(task)
                event_details[task] = {"id": event.id, "topic": topic_path}

            except Exception as e:
                log.error(
                    "Failed to initiate publishing for event "
                    f"{getattr(event, 'id', 'unknown')}: {e}",
                    exc_info=e,
                )

        if not publish_tasks:
            log.warning("No publish tasks were successfully created for the batch.")
            return BatchPublishResult(success_ids=[], failure_ids=[event.id for event in events])

        log.info(f"Waiting for {len(publish_tasks)} publish operations to complete...")
        results = await asyncio.gather(*publish_tasks, return_exceptions=True)

        published = []
        failed = []
        for i, result in enumerate(results):
            task = publish_tasks[i]
            details = event_details[task]
            event_id: str = details["id"]
            topic_path = details["topic"]

            if isinstance(result, Exception):
                failed.append(event_id)
                if isinstance(result, TimeoutError):
                    log.error(f"Timed out publishing event {event_id} to {topic_path}")
                else:
                    log.error(
                        f"Failed to publish event {event_id} to {topic_path}: {result}",
                        exc_info=result,
                    )
            else:
                published.append(event_id)
                log.debug(
                    f"Successfully published event {event_id} to "
                    f"{topic_path} with message ID {result}"
                )

        if failed:
            log.error(f"Failed to publish {len(failed)} out of {len(events)} events in the batch.")
        if published:
            log.info(
                f"Successfully published {len(published)} out of {len(events)} events in the batch."
            )
        return BatchPublishResult(success_ids=list(published), failure_ids=list(failed))
