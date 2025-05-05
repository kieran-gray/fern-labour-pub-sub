from dataclasses import dataclass
from typing import Any, Protocol


class EventHandler(Protocol):
    """Protocol for event handlers"""

    async def handle(self, event: dict[str, Any]) -> None:
        """Handle the event."""


@dataclass
class TopicHandler:
    topic: str
    event_handler: type[EventHandler]
    component: str = ""

    @property
    def sub(self) -> str:
        return f"{self.topic}.sub"
