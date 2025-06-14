from typing import Protocol


class IdempotencyStore(Protocol):
    """
    An abstract store to ensure message processing is idempotent.
    """

    async def is_duplicate(self, key: str, context: str) -> bool:
        """
        Atomically checks if a key has been processed and marks it as such.

        Args:
            key: The unique idempotency key (e.g., event_id).
            context: The context of the processing (e.g., subscription_name).

        Returns:
            True if the key is a duplicate.
            False if this is the first time seeing the key.
        """
