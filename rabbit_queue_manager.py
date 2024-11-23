import pika
from pika import BasicProperties
from pika.spec import Basic
from pika.exceptions import ProbableAuthenticationError
from pika.exceptions import StreamLostError
from pika.exceptions import ChannelClosedByBroker


RABBITMQ_USER="username"
RABBITMQ_PASSWORD="password"
RABBITMQ_ADDRESS="127.0.0.1"

class RabbitQueueManager:
    
    def __init__(self, queue_name: str) -> None:
        self._queue_name = queue_name
        self._connection = None
        self._open_connection()

    def _open_connection(self) -> None:
        try:
            credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
            self._connection = pika.BlockingConnection(pika.ConnectionParameters(RABBITMQ_ADDRESS, credentials=credentials))
            
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=self._queue_name)
            
        except ProbableAuthenticationError:
            print("Rabbit MQ. Login or password is wrong")
        except Exception:
            print("Rabbit MQ. Uknown error")
            
    def _close(self) -> None:
        if self._connection and self._connection.is_open:
            self._connection.close()

    def _receive_message(self) -> str | None:
        if not (self._connection and self._connection.is_open):
            print("RabbitMQ: Connection is closed. Reconnecting.")
            self._open_connection()
        
        try:
            method_frame, _, body = self._channel.basic_get(self._queue_name) # type: ignore
        except ChannelClosedByBroker:
            print(f"RabbitMQ: Queue '{self._queue_name}' not found.")
            self._close()
        
        except StreamLostError:
            print("RabbitMQ: Connection lost. Reconnecting.")
            self._open_connection()
            return self._receive_message()
        
        # If there is no message in the queue
        if method_frame is None:
            return None
        
        try:
            ok: Basic.GetOk = method_frame
            data: BasicProperties = body

            self._channel.basic_ack(ok.delivery_tag)
            return data.decode() # type: ignore

        except Exception as _:
            print("Rabbit MQ. Receiving error")
            self._close()
            return None

    def add_to_queue(self, item: str) -> None:
        try:
            self._channel.basic_publish(exchange='', routing_key=self._queue_name, body=item)

        except StreamLostError:
            print("Rabbit MQ. Connection lost. Reconnecting.")
            self._open_connection()
            self.add_to_queue(item)

        except Exception:
            print("Rabbit MQ. Uknown error")
    
    def get_size(self) -> int:
        if not (self._connection and self._connection.is_open):
            print("RabbitMQ: Connection is closed. Reconnecting.")
            self._open_connection()
            
        _method_frame = self._channel.queue_declare(queue=self._queue_name, passive=True)
        return _method_frame.method.message_count

    def add_list_to_queue(self, items: list[str]) -> None:
        for item in items:
            self.add_to_queue(item)
        
    def get_next(self) -> str | None:
        return self._receive_message()

    # Add support for iteration
    def __iter__(self):
        return self

    def __next__(self) -> str | None:
        message = self._receive_message()
        if message is None:
            raise StopIteration
        return message

    # Add support for len()
    def __len__(self) -> int:
        return self.get_size()
    

