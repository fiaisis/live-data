class TopicIncompleteError(RuntimeError):
    """Raised when the Kafka topic is not yet complete"""

class SampleLogError(RuntimeError):
    """Raised when there is an error with the sample log"""