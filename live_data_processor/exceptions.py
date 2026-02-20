class TopicIncompleteError(RuntimeError):
    """Raised when the Kafka topic is not yet complete"""


class SampleLogError(RuntimeError):
    """Raised when there is an error with the sample log"""


class OffsetNotFoundError(RuntimeError):
    """Raised when the offset is not found"""
