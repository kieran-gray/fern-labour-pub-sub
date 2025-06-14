from collections.abc import AsyncGenerator, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Self

import pytest_asyncio
from dishka import AsyncContainer, Provider, Scope, make_async_container, provide
from fern_labour_core.events.event import DomainEvent
from fern_labour_core.events.event_handler import EventHandler
from fern_labour_core.unit_of_work import UnitOfWork

from fern_labour_pub_sub.idempotency_store import (
    AlreadyCompletedError,
    IdempotencyStore,
    LockContentionError,
)


@dataclass
class MockEvent(DomainEvent):
    id: str
    type: str
    aggregate_id: str
    aggregate_type: str
    data: dict[str, Any]
    time: datetime

    @classmethod
    def create(
        cls, aggregate_id: str, aggregate_type: str, data: dict[str, Any], event_type: str = ""
    ) -> Self:
        return cls(
            id="evt-123",
            type=event_type,
            aggregate_id=aggregate_id,
            aggregate_type=aggregate_type,
            data=data,
            time=datetime(2020, 1, 1, 12),
        )

    @classmethod
    def from_dict(cls, event: dict[str, Any]) -> Self:
        return cls(
            id=event["id"],
            type=event["type"],
            aggregate_id=event["aggregate_id"],
            aggregate_type=event["aggregate_type"],
            data=event["data"],
            time=datetime.fromisoformat(event["time"]),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "type": self.type,
            "aggregate_id": self.aggregate_id,
            "aggregate_type": self.aggregate_type,
            "data": self.data,
            "time": self.time.isoformat(),
        }


class MockIdempotencyStore(IdempotencyStore):
    _data: dict[str, tuple[str, datetime, datetime | None]] = {}

    async def try_claim_event(self, event_id: str) -> None:
        existing_row = self._data.get(event_id)
        if not existing_row:
            self._data[event_id] = (event_id, datetime.now(UTC), None)
            return

        if existing_row[2]:
            raise AlreadyCompletedError()
        raise LockContentionError()

    async def mark_as_completed(self, event_id: str) -> None:
        existing_row = self._data[event_id]
        self._data[event_id] = (event_id, existing_row[1], datetime.now(UTC))


class MockUnitOfWork(UnitOfWork):
    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        return None

    async def commit(self) -> None:
        return None

    async def rollback(self) -> None:
        return None


class MockDefaultEventHandler(EventHandler):
    async def handle(self, event: dict[str, Any]) -> None:
        return


class MockEventHandler(EventHandler):
    async def handle(self, event: dict[str, Any]) -> None:
        return


class MockFailedEventHandler(EventHandler):
    async def handle(self, event: dict[str, Any]) -> None:
        raise Exception("Test")


class MockDefaultProvider(Provider):
    scope = Scope.APP
    component = ""

    @provide
    def get_mock_default_event_handler(self) -> MockDefaultEventHandler:
        return MockDefaultEventHandler()

    @provide
    def get_mock_idempotency_store(self) -> IdempotencyStore:
        return MockIdempotencyStore()

    @provide
    def get_mock_unit_of_work(self) -> UnitOfWork:
        return MockUnitOfWork()


class MockEventHandlerProvider(Provider):
    scope = Scope.APP
    component = "event_handlers"

    @provide
    def get_mock_event_handler(self) -> MockEventHandler:
        return MockEventHandler()

    @provide
    def get_mock_failed_event_handler(self) -> MockFailedEventHandler:
        return MockFailedEventHandler()


def get_providers() -> Iterable[Provider]:
    return (
        MockDefaultProvider(),
        MockEventHandlerProvider(),
    )


@pytest_asyncio.fixture(scope="session")
async def container() -> AsyncGenerator[AsyncContainer]:
    """Create a test dishka container."""
    container = make_async_container(*get_providers())
    yield container
    await container.close()
