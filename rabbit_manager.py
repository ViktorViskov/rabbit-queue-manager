import logging

import pika
from pika.exceptions import (
    NackError,
    ProbableAuthenticationError,
    StreamLostError,
    UnroutableError,
)

logging.basicConfig(
    level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class RabbitManager:
    def __init__(
        self,
        queue_name: str,
        *,
        username: str,
        password: str,
        host: str = "localhost",
        port: int = 5672,
        queue_durable: bool = True,
        message_ttl_minutes: int = 0,
        confirm_delivery: bool = True,
    ) -> None:
        self._queue_name = queue_name
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._queue_durable = queue_durable
        self._connection = None
        self._channel = None
        self._message_ttl_minutes = message_ttl_minutes
        self._confirm_delivery = confirm_delivery

    def open(self) -> None:
        try:
            credentials = pika.PlainCredentials(self._username, self._password)
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    self._host,
                    self._port,
                    credentials=credentials,
                    connection_attempts=5,
                    retry_delay=5,
                    socket_timeout=10,
                    heartbeat=30,
                    blocked_connection_timeout=60,
                )
            )

            arguments = {}
            if self._message_ttl_minutes > 0:
                arguments["x-message-ttl"] = self._message_ttl_minutes * 60 * 1000

            self._channel = self._connection.channel()

            # Enable publisher confirms for delivery guarantees
            if self._confirm_delivery:
                self._channel.confirm_delivery()

            self._channel.queue_declare(
                queue=self._queue_name,
                durable=self._queue_durable,
                arguments=arguments,
            )

        except ProbableAuthenticationError as e:
            logger.error("RabbitMQ authentication failed: Login or password is wrong")
            raise e
        except Exception as e:
            logger.exception("Failed to connect to RabbitMQ")
            raise e

    def close(self) -> None:
        """Close the connection to RabbitMQ."""
        if self._connection and not self._connection.is_closed:
            try:
                self._connection.close()
                logger.info("RabbitMQ connection closed successfully")
            except Exception as e:
                logger.error(f"Error closing RabbitMQ connection: {e}")

    def __enter__(self):
        self.open()
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
        return False

    def add(self, item: str) -> bool:
        """Add a message to the queue.

        Returns:
            bool: True if message was successfully delivered, False otherwise.
        """
        if self._connection is None or self._connection.is_closed:
            logger.warning("RabbitMQ connection is closed. Reconnecting...")
            self.open()

        if self._channel is None:
            raise Exception("RabbitMQ channel is not established.")

        try:
            # With confirm_delivery enabled, this will raise an exception
            # if the message cannot be delivered
            self._channel.basic_publish(
                exchange="",
                routing_key=self._queue_name,
                body=item,
                mandatory=True,  # Return message if it can't be routed
            )
            logger.debug(
                f"Message successfully published to queue '{self._queue_name}'"
            )
            return True

        except StreamLostError as e:
            logger.warning("Connection lost.")
            raise e
        except UnroutableError as e:
            logger.error(f"Message could not be routed to queue: {e}")
            return False
        except NackError as e:
            logger.error(f"Message was rejected by broker: {e}")
            return False
        except Exception as e:
            logger.exception("Error adding message to RabbitMQ")
            raise e

    def size(self) -> int:
        """Get the number of messages in the queue.

        Returns:
            int: Number of messages in the queue.
        """
        if self._connection is None or self._connection.is_closed:
            logger.warning("RabbitMQ connection is closed. Reconnecting...")
            self.open()

        if self._channel is None:
            raise Exception("RabbitMQ channel is not established.")

        try:
            queue = self._channel.queue_declare(queue=self._queue_name, passive=True)
            message_count = queue.method.message_count
            logger.debug(f"Queue '{self._queue_name}' has {message_count} messages")
            return message_count

        except Exception as e:
            logger.exception("Error getting queue size from RabbitMQ")
            raise e

    def get(self) -> str | None:
        """Get a message from the queue.

        Returns:
            str: Message body if available, None if queue is empty.
        """
        if self._connection is None or self._connection.is_closed:
            logger.warning("RabbitMQ connection is closed. Reconnecting...")
            self.open()

        if self._channel is None:
            raise Exception("RabbitMQ channel is not established.")

        try:
            # Set QoS to 1 - acknowledge messages one at a time
            self._channel.basic_qos(prefetch_count=1)

            # Get one message from the queue (non-blocking)
            method, properties, body = self._channel.basic_get(
                queue=self._queue_name, auto_ack=True
            )

            # If no message is available, basic_get returns (None, None, None)
            if method is None:
                logger.debug(f"No messages in queue '{self._queue_name}'")
                return None

            message = body.decode("utf-8")
            logger.debug(f"Message retrieved from queue '{self._queue_name}'")
            return message

        except StreamLostError as e:
            logger.warning("Connection lost.")
            raise e
        except Exception as e:
            logger.exception("Error getting message from RabbitMQ")
            raise e

    def consume(self, timeout: int | None = None) -> str | None:
        """Wait (block) for a message from the queue using consume().

        This method BLOCKS until a message arrives or timeout expires.

        Args:
            timeout: Maximum seconds to wait for a message. None = wait forever.

        Returns:
            str: Message body when received, None if timeout expired.
        """
        if self._connection is None or self._connection.is_closed:
            logger.warning("RabbitMQ connection is closed. Reconnecting...")
            self.open()

        if self._channel is None:
            raise Exception("RabbitMQ channel is not established.")

        try:
            # Set QoS to 1 - acknowledge messages one at a time
            self._channel.basic_qos(prefetch_count=1)

            logger.debug(f"Waiting for message from queue '{self._queue_name}'...")

            # BLOCKING method - waits for a message
            for method, properties, body in self._channel.consume(
                queue=self._queue_name,
                inactivity_timeout=timeout,  # Timeout in seconds (None = wait forever)
                auto_ack=True,
            ):
                # If timeout expired, method is None
                if method is None:
                    logger.debug(f"Timeout: no message received in {timeout} seconds")
                    # Cancel the consumer
                    self._channel.cancel()
                    return None

                # Message received
                message = body.decode("utf-8")
                logger.debug(f"Message received from queue '{self._queue_name}'")

                # Cancel the consumer after getting one message
                self._channel.cancel()
                return message

        except StreamLostError as e:
            logger.warning("Connection lost.")
            raise e
        except Exception as e:
            logger.exception("Error listening to RabbitMQ")
            raise e
