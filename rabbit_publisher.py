import json

import pika

from pika.delivery_mode import DeliveryMode

from app.libs.transport.rabbit.rabbit_connection import RabbitConnection


class RabbitPublisher:
    """Class for publishing messages to rabbit."""
    def __init__(
            self,
            queue_name: str,
            exchange_name: str,
            pika_delivery_mode: DeliveryMode = pika.DeliveryMode.Persistent,
            mandatory: bool = False,
    ) -> None:
        """
        Initializes rabbit channel.
        Creates params for basic_publish.
        """
        self.__connection = RabbitConnection.get_publisher_connection()
        self.__channel = self.__connection.channel()
        self.__exchange_name = exchange_name
        self.__queue_name = queue_name
        self.__pika_delivery_mode = pika_delivery_mode
        self.__mandatory = mandatory

    def send_message(self, message: dict) -> None:
        """Sends message to rabbit."""
        self.__channel.basic_publish(
            exchange=self.__exchange_name,
            routing_key=self.__queue_name,
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=self.__pika_delivery_mode
            ),
            mandatory=self.__mandatory
        )
        self.__close_channel()
        self.__close_connection()

    def __close_channel(self):
        """Closes rabbit channel."""
        self.__channel.close()

    def __close_connection(self):
        """Closes rabbit connection."""
        self.__connection.close()

