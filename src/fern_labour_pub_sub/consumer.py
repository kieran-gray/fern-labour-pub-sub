import asyncio
import functools
import json
import logging
from concurrent.futures import Future

from dishka import AsyncContainer, Scope
from google.cloud import pubsub_v1
from google.cloud.pubsub_v1.subscriber.futures import StreamingPullFuture
from google.cloud.pubsub_v1.subscriber.message import Message

from fern_labour_pub_sub.enums import ConsumerMode
from fern_labour_pub_sub.topic_handler import TopicHandler

log = logging.getLogger(__name__)


class PubSubEventConsumer:
    """
    Google Pub/Sub-based event consumer
    """

    def __init__(
        self,
        project_id: str,
        topic_handlers: list[TopicHandler],
        subscriber: pubsub_v1.SubscriberClient | None = None,
        mode: ConsumerMode = ConsumerMode.STREAMING_PULL,
        batch_max_messages: int = 50,
    ):
        """
        Initialize PubSubEventConsumer.

        Args:
            project_id: Google Cloud project ID.
            topic_prefix: Prefix for Pub/Sub topics.
            subscriber: Optional pre-configured SubscriberClient.
            mode: Optional running mode
            batch_max_messages: Optional max messages to process in a batch in UnaryPull mode

        Consumer modes:
            StreamingPull mode, consumer will:
                - Subscribe to topics.
                - Begin processing messages.
                - Keep running until stop() is called.

            UnaryPull mode, consumer will:
                - Sequentially pull at most batch_max_messages for each subscription.
                - Close on completion.
        """
        self._project_id = project_id
        self._subscriber = subscriber or pubsub_v1.SubscriberClient()
        self._handlers: dict[str, TopicHandler] = {
            handler.sub: handler for handler in topic_handlers
        }
        self._mode = mode
        self._batch_max_messages = batch_max_messages
        self._running = False
        self._container: AsyncContainer | None = None
        self._streaming_pull_futures: dict[str, StreamingPullFuture] = {}
        self._loop: asyncio.AbstractEventLoop | None = None
        self._tasks: set[Future[None]] = set()

    def set_container(self, container: AsyncContainer) -> None:
        """
        Set the dependency injection container.

        Args:
            container: AsyncContainer with scope=APP.
        """
        self._container = container
        log.info("Dependency injection container set for PubSubEventConsumer.")

    def _get_subscription_path(self, topic: str) -> str:
        """
        Get the full subscription path for a topic.

        Args:
            topic: The topic name.

        Returns:
            Full subscription path.
        """
        subscription_path = f"{topic}.sub"
        return str(self._subscriber.subscription_path(self._project_id, subscription_path))

    async def _process_message(self, message: Message) -> None:
        try:
            data_str = message.data.decode("utf-8")
            event = json.loads(data_str)
            topic = event["type"]
            log.debug(f"Message data for topic {topic}: {data_str}")
        except json.JSONDecodeError as e:
            log.exception(f"Failed to decode JSON message data for {message.data!r}", exc_info=e)
            message.nack()
            return

        subscription = f"{topic}.sub"
        topic_handler = self._handlers.get(subscription)
        if not topic_handler:
            log.error(
                "No handler found for message from subscription related to ack_id: "
                f"{message.ack_id}. Attempted path match: {subscription}"
            )
            message.nack()
            return

        log.info(f"Received message for topic: {topic} (Subscription: {subscription})")

        if not self._container:
            log.error("Dependency injection container not set. Cannot process messages.")
            message.nack()
            return

        try:
            async with self._container(scope=Scope.REQUEST) as request_container:
                event_handler = await request_container.get(
                    topic_handler.event_handler, component=topic_handler.component
                )
                await event_handler.handle(event)
            message.ack()
            log.info(f"Successfully processed and ACKed message for topic: {topic}")
        except Exception as e:
            log.exception(f"Error processing message for topic {topic}.", exc_info=e)
            message.nack()

    def _message_callback(self, message: Message) -> None:
        """
        Callback function executed by the Pub/Sub library's thread pool
        for each received message. Bridges the sync callback to async processing.

        Args:
            message: The received Pub/Sub message.
        """
        if not self._running:
            log.warning(f"Consumer not running, NACKing message: {message.message_id}")
            message.nack()
            return

        if not self._loop:
            log.error("Event loop not available in callback. Cannot schedule async processing.")
            message.nack()
            return

        coro = self._process_message(message)
        future = asyncio.run_coroutine_threadsafe(coro, self._loop)

        self._tasks.add(future)
        future.add_done_callback(lambda f: self._tasks.remove(f))

    def _on_future_done(self, subscription_path: str, future: StreamingPullFuture) -> None:
        """
        Callback executed when a StreamingPullFuture finishes (completes, errors, or is cancelled).
        Runs in the Pub/Sub library's thread pool.
        """
        try:
            future.result(timeout=0)
            log.info(
                f"Streaming pull future for {subscription_path}"
                " completed normally (likely cancelled)."
            )
        except Exception as e:
            log.exception(f"Streaming pull future for {subscription_path} failed!", exc_info=e)
            if self._running:
                log.error(f"Subscription {subscription_path} may no longer be active due to error.")

        if subscription_path in self._streaming_pull_futures:
            if self._streaming_pull_futures[subscription_path] == future:
                del self._streaming_pull_futures[subscription_path]
                log.info(f"Removed completed/failed future for {subscription_path}.")

    async def start(self) -> None:
        """
        Start the Pub/Sub consumer.
        """
        if self._running:
            log.warning("Consumer is already running.")
            return

        if not self._handlers:
            log.error("No event handlers registered. Cannot start consumer.")
            return

        if not self._container:
            log.error("Dependency injection container not set. Cannot start consumer.")
            return

        if self._mode is ConsumerMode.STREAMING_PULL:
            await self._start_streaming_pull_consumer()
        elif self._mode is ConsumerMode.UNARY_PULL:
            with self._subscriber:
                await self._run_unary_pull_consumer()

    async def _start_streaming_pull_consumer(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._running = True
        self._streaming_pull_futures.clear()
        log.info(f"Starting PubSubEventConsumer for project {self._project_id}...")

        for topic_handler in self._handlers.values():
            subscription = topic_handler.sub
            topic = topic_handler.topic
            log.info(f"Attempting to subscribe to: {subscription} (for topic: {topic})")

            try:
                done_callback = functools.partial(self._on_future_done, subscription)

                streaming_pull_future = self._subscriber.subscribe(
                    subscription=self._get_subscription_path(topic=topic),
                    callback=self._message_callback,
                )
                streaming_pull_future.add_done_callback(done_callback)
                self._streaming_pull_futures[subscription] = streaming_pull_future
                log.info(f"Successfully subscribed to {subscription}. Future started.")

            except Exception as e:
                log.exception(f"Failed to subscribe to {subscription}", exc_info=e)

        if not self._streaming_pull_futures:
            log.error("No subscriptions were successfully started. Stopping consumer.")
            self._running = False
            return

        log.info(
            "PubSubEventConsumer started. Monitoring "
            f"{len(self._streaming_pull_futures)} subscriptions."
        )

        while self._running:
            await asyncio.sleep(5)

            if self._running and not self._streaming_pull_futures:
                log.warning("Consumer is running but has no active subscriptions left. Stopping.")
                await self.stop()

    async def _run_unary_pull_consumer(self) -> None:
        for topic_handler in self._handlers.values():
            topic = topic_handler.topic
            subscription = topic_handler.sub
            try:
                response = self._subscriber.pull(
                    request={
                        "subscription": self._get_subscription_path(topic=topic),
                        "max_messages": self._batch_max_messages,
                        "return_immediately": True,
                    },
                )
            except Exception as e:
                log.exception(f"Failed to pull subscription {subscription}", exc_info=e)
                continue

            if len(response.received_messages) == 0:
                log.info(f"No messages pulled for subscription {subscription}")
                continue
            log.info(f"Pulled {len(response.received_messages)} for subscription {subscription}")
            for message in response.received_messages:
                await self._process_message(message=message)

    async def stop(self) -> None:
        """
        Stop the Pub/Sub consumer gracefully.
        """
        if not self._running:
            log.info("PubSubEventConsumer is not running.")
            return

        log.info("Stopping PubSubEventConsumer...")
        self._running = False

        futures_to_cancel = list(self._streaming_pull_futures.values())
        if futures_to_cancel:
            log.info(f"Cancelling {len(futures_to_cancel)} subscription future(s)...")
            for future in futures_to_cancel:
                future.cancel()

            await asyncio.sleep(1)

        if self._subscriber:
            log.info("Closing Pub/Sub subscriber client...")
            try:
                await asyncio.get_running_loop().run_in_executor(None, self._subscriber.close)
                log.info("Pub/Sub subscriber client closed.")
            except Exception as e:
                log.exception("Error closing Pub/Sub subscriber client.", exc_info=e)

        self._streaming_pull_futures.clear()
        if self._tasks:
            log.info(f"Waiting for {len(self._tasks)} message processing tasks to complete...")
            await asyncio.wait(self._tasks, timeout=10)  # type: ignore

        log.info("PubSubEventConsumer stopped.")

    async def is_healthy(self) -> bool:
        """
        Check if the Pub/Sub consumer is healthy.
        Checks if running, subscriber exists, and futures are active.

        Returns:
            True if healthy, otherwise False.
        """
        if not self._running:
            log.debug("Health check: Consumer not running.")
            return False

        if not self._subscriber:
            log.warning("Health check: Pub/Sub subscriber is not initialized.")
            return False

        if self._mode is ConsumerMode.STREAMING_PULL:
            if not self._streaming_pull_futures:
                log.warning("Health check: No active subscription futures.")
                return False

            any_running = False
            for sub_path, future in self._streaming_pull_futures.items():
                if future.running():
                    any_running = True
                    log.debug(f"Health check: Subscription {sub_path} future is running.")

            if not any_running:
                log.warning(
                    "Health check: Consumer is running, but no subscription futures are active."
                )
                return False

        return True
