from dataclasses import dataclass

from fern_labour_core.events.event_handler import EventHandler


@dataclass
class TopicHandler:
    topic: str
    event_handler: type[EventHandler]
    component: str = ""

    @property
    def sub(self) -> str:
        return f"{self.topic}.sub"
