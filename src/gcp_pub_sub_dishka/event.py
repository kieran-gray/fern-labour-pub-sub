from datetime import datetime
from typing import Any, Protocol, Self

class Event(Protocol):
    """Base class for all events"""

    id: str
    type: str
    data: dict[str, Any]
    time: datetime

    @classmethod
    def create(cls, data: dict[str, Any], event_type: str = "") -> Self: ...

    @classmethod
    def from_dict(cls, event: dict[str, Any]) -> Self: ...

    def to_dict(self) -> dict[str, Any]: ...
