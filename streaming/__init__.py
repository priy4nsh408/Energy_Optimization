"""Kafka streaming layer.

Modules:
    topics          topic registry and creation (``python -m streaming.topics``)
    kafka_config    connection helpers (error callbacks, broker wait)
    serialization   JSON encoding of message values and keys
    producer        :class:`~streaming.producer.JsonProducer`
    consumer        :class:`~streaming.consumer.JsonConsumer`

Submodules are imported explicitly (no eager imports here) so that
``python -m streaming.topics`` runs cleanly.
"""
