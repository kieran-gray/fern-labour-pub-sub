from enum import StrEnum


class ConsumerMode(StrEnum):
    STREAMING_PULL = "streaming_pull"
    UNARY_PULL = "unary_pull"
