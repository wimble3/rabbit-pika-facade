import logging
from abc import ABC, abstractmethod

from pika.exceptions import ChannelClosed
from pika.exchange_type import ExchangeType

from app.libs.transport.rabbit.rabbit_connection import RabbitConnection


class RabbitConsumer(ABC):
    """Base abstract class for rabbit consumers."""
    def __init__(
            self,
            queue_name: str,
            exchange_name: str,

            durable: bool = True,
            passive: bool = False,
            exclusive: bool = False,
            auto_delete: bool = False,
            arguments: dict | None = None,

            exchange_type: ExchangeType = ExchangeType.fanout,
            exchange_passive: bool = False,
            exchange_durable: bool = False,
            exchange_auto_delete: bool = False,
            exchange_internal: bool = False,
            exchange_arguments: dict | None = None,
    ) -> None:
        """
        Initializes rabbit channel.
        Declares rabbit queue and exchange.
        """
        self.__connection = RabbitConnection.get_consumers_connection()
        self._channel = self.__connection.channel()
        self.__queue_name = queue_name
        self._channel.queue_declare(
            queue=queue_name,
            durable=durable,
            passive=passive,
            exclusive=exclusive,
            auto_delete=auto_delete,
            arguments=arguments)
        self._channel.exchange_declare(
            exchange=exchange_name,
            exchange_type=exchange_type,
            passive=exchange_passive,
            durable=exchange_durable,
            auto_delete=exchange_auto_delete,
            internal=exchange_internal,
            arguments=exchange_arguments)
        self._delivery_tag = None
        self._durable = durable

    def start_consume(self):
        """Starts consuming process.

        Subscribes on rabbit queue.
        """
        try:
            self._channel.basic_consume(
                queue=self.__queue_name,
                on_message_callback=self.on_message)
            self._channel.start_consuming()
        except ChannelClosed:
            logging.info(
                f"Queue {self.__queue_name}: Channel has been closed, restart")
            self._channel = self.__connection.channel()
            self.__restart()

    def __restart(self) -> None:
        """Restart consuming process."""
        self._channel.stop_consuming()
        self._channel.start_consuming()

    def _ack(self):
        """Acknowledgement from consumer about receiving message."""
        logging.info(f"Queue {self.__queue_name}: Ack")
        self._channel.basic_ack(self._delivery_tag)

    def _reject(self, requeue: bool = False):
        """Rejects message from rabbit."""
        logging.info(f"Queue {self.__queue_name}:Reject")
        self._channel.basic_reject(self._delivery_tag, requeue)

    @abstractmethod
    def on_message(self, channel, method, properties, body):
        """Callback function called when message received from rabbit."""
