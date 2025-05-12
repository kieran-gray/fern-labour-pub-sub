from typing import Any


class MessageProcessingException(Exception):
    def __init__(self, message_id: Any):
        super().__init__(f"Failed to process message id '{message_id}")
