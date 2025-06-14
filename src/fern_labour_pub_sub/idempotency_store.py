from typing import Protocol


class IdempotencyException(Exception):
    """Base exception for idempotency flows."""

    pass


class AlreadyCompletedError(IdempotencyException):
    """Raised when an event has already been successfully processed."""

    pass


class LockContentionError(IdempotencyException):
    """Raised when another consumer has a lock on the event."""

    pass


class IdempotencyStore(Protocol):
    """
    An abstract store to ensure message processing is idempotent.
    """

    async def try_claim_event(self, event_id: str) -> None:
        """
        Claims an event for processing using a non-blocking lock.

        Raises:
            AlreadyCompletedError: If the event was already completed.
            LockContentionError: If another process holds the lock.
        """

    async def mark_as_completed(self, event_id: str) -> None:
        """Marks the event as completed in the database."""
