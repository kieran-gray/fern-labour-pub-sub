from typing import Any, Protocol


class EventHandler(Protocol):
    """Protocol for event handlers"""

    async def handle(self, event: dict[str, Any]) -> None:
        """Handle the event."""
