import pika

from pika.adapters.blocking_connection import BlockingConnection

from settings import settings


class RabbitConnection:
    """
    Class for representing a singleton instance of sames rabbit connections.
    """
    __consumers_connection: BlockingConnection | None = None
    CREDENTIALS = pika.PlainCredentials(settings.RMQ_USER, settings.RMQ_PASS)

    def __init__(self):
        """Empty init method.

        Raises:
            TypeError if you want to create class object.
        """
        raise TypeError(
            f"{RabbitConnection.__name__} class cannot be instantiated.")

    @classmethod
    def get_consumers_connection(cls) -> BlockingConnection:
        """Get the singleton instance of rabbit connection for consumers."""
        if (cls.__consumers_connection is None or
                cls.__consumers_connection.is_closed):
            cls.__consumers_connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    settings.RMQ_HOST, credentials=cls.CREDENTIALS))
        return cls.__consumers_connection

    @classmethod
    def get_publisher_connection(cls) -> BlockingConnection:
        """Get the singleton instance of rabbit connection for publishers."""
        return pika.BlockingConnection(pika.ConnectionParameters(
            settings.RMQ_HOST, credentials=cls.CREDENTIALS))
