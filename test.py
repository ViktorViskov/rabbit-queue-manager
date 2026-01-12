import pytest
from unittest.mock import Mock, patch
from pika.exceptions import (
    ProbableAuthenticationError,
    StreamLostError,
    UnroutableError,
    NackError,
)
from rabbit_manager import RabbitManager


class TestRabbitManagerInit:
    """Tests for RabbitManager initialization."""

    def test_init_default_values(self):
        """Test initialization with default values."""
        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

        assert manager._host == "localhost"
        assert manager._port == 5672
        assert manager._username == "guest"
        assert manager._password == "guest"
        assert manager._queue_name == "test_queue"
        assert manager._queue_durable is True
        assert manager._message_ttl_minutes == 0
        assert manager._confirm_delivery is True
        assert manager._connection is None
        assert manager._channel is None

    def test_init_custom_values(self):
        """Test initialization with custom values."""
        manager = RabbitManager(
            host="rabbitmq.example.com",
            port=5673,
            username="admin",
            password="secret",
            queue_name="custom_queue",
            queue_durable=False,
            message_ttl_minutes=30,
            confirm_delivery=False,
        )

        assert manager._host == "rabbitmq.example.com"
        assert manager._port == 5673
        assert manager._username == "admin"
        assert manager._password == "secret"
        assert manager._queue_name == "custom_queue"
        assert manager._queue_durable is False
        assert manager._message_ttl_minutes == 30
        assert manager._confirm_delivery is False


class TestRabbitManagerOpen:
    """Tests for the open() method."""

    @patch("rabbit_manager.pika.BlockingConnection")
    def test_open_success(self, mock_connection_class):
        """Test successful connection opening."""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection

        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

        manager.open()

        assert manager._connection == mock_connection
        assert manager._channel == mock_channel
        mock_channel.confirm_delivery.assert_called_once()
        mock_channel.queue_declare.assert_called_once_with(
            queue="test_queue", durable=True, arguments={}
        )

    @patch("rabbit_manager.pika.BlockingConnection")
    def test_open_with_ttl(self, mock_connection_class):
        """Test connection opening with TTL."""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection

        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
            message_ttl_minutes=10,
        )

        manager.open()

        mock_channel.queue_declare.assert_called_once_with(
            queue="test_queue",
            durable=True,
            arguments={"x-message-ttl": 600000},  # 10 minutes in ms
        )

    @patch("rabbit_manager.pika.BlockingConnection")
    def test_open_without_confirm_delivery(self, mock_connection_class):
        """Test opening without confirm_delivery."""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection_class.return_value = mock_connection

        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
            confirm_delivery=False,
        )

        manager.open()

        mock_channel.confirm_delivery.assert_not_called()

    @patch("rabbit_manager.pika.BlockingConnection")
    def test_open_authentication_error(self, mock_connection_class):
        """Test authentication error."""
        mock_connection_class.side_effect = ProbableAuthenticationError()

        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="wrong",
            password="wrong",
            queue_name="test_queue",
        )

        with pytest.raises(ProbableAuthenticationError):
            manager.open()


class TestRabbitManagerClose:
    """Tests for the close() method."""

    def test_close_success(self):
        """Test successful connection closing."""
        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

        mock_connection = Mock()
        mock_connection.is_closed = False
        manager._connection = mock_connection

        manager.close()

        mock_connection.close.assert_called_once()

    def test_close_already_closed(self):
        """Test closing an already closed connection."""
        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

        mock_connection = Mock()
        mock_connection.is_closed = True
        manager._connection = mock_connection

        manager.close()

        mock_connection.close.assert_not_called()

    def test_close_no_connection(self):
        """Test closing with no connection."""
        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

        manager.close()  # Should not raise any errors


class TestRabbitManagerContextManager:
    """Tests for context manager."""

    @patch("rabbit_manager.pika.BlockingConnection")
    def test_context_manager(self, mock_connection_class):
        """Test working as context manager."""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_connection_class.return_value = mock_connection

        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

        with manager as m:
            assert m == manager
            assert manager._connection == mock_connection

        mock_connection.close.assert_called_once()


class TestRabbitManagerAdd:
    """Tests for the add() method."""

    def setup_method(self):
        """Setup for each test."""
        self.manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

    def test_add_success(self):
        """Test successful message addition."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        result = self.manager.add("test message")

        assert result is True
        mock_channel.basic_publish.assert_called_once_with(
            exchange="",
            routing_key="test_queue",
            body="test message",
            mandatory=True,
        )

    @patch.object(RabbitManager, "open")
    def test_add_reconnects_when_closed(self, mock_open):
        """Test reconnection when connection is closed."""
        mock_connection = Mock()
        mock_connection.is_closed = True
        mock_channel = Mock()

        self.manager._connection = mock_connection

        # Simulate connection opening
        def side_effect():
            self.manager._connection = Mock()
            self.manager._connection.is_closed = False
            self.manager._channel = mock_channel

        mock_open.side_effect = side_effect

        result = self.manager.add("test message")

        mock_open.assert_called_once()
        assert result is True

    def test_add_no_channel(self):
        """Test error when channel is missing."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        self.manager._connection = mock_connection
        self.manager._channel = None

        with pytest.raises(Exception, match="RabbitMQ channel is not established"):
            self.manager.add("test message")

    def test_add_unroutable_error(self):
        """Test routing error."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.basic_publish.side_effect = UnroutableError([Mock()])

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        result = self.manager.add("test message")

        assert result is False

    def test_add_nack_error(self):
        """Test message rejection error."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.basic_publish.side_effect = NackError([Mock()])

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        result = self.manager.add("test message")

        assert result is False

    def test_add_stream_lost_error(self):
        """Test connection loss error."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.basic_publish.side_effect = StreamLostError()

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        with pytest.raises(StreamLostError):
            self.manager.add("test message")


class TestRabbitManagerSize:
    """Tests for the size() method."""

    def setup_method(self):
        """Setup for each test."""
        self.manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

    def test_size_success(self):
        """Test getting queue size."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()

        mock_queue_declare = Mock()
        mock_queue_declare.method.message_count = 42
        mock_channel.queue_declare.return_value = mock_queue_declare

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        size = self.manager.size()

        assert size == 42
        mock_channel.queue_declare.assert_called_once_with(
            queue="test_queue", passive=True
        )

    @patch.object(RabbitManager, "open")
    def test_size_reconnects_when_closed(self, mock_open):
        """Test reconnection when connection is closed."""
        mock_connection = Mock()
        mock_connection.is_closed = True

        self.manager._connection = mock_connection

        # Simulate connection opening
        def side_effect():
            self.manager._connection = Mock()
            self.manager._connection.is_closed = False
            mock_channel = Mock()
            mock_queue_declare = Mock()
            mock_queue_declare.method.message_count = 10
            mock_channel.queue_declare.return_value = mock_queue_declare
            self.manager._channel = mock_channel

        mock_open.side_effect = side_effect

        size = self.manager.size()

        mock_open.assert_called_once()
        assert size == 10

    def test_size_no_channel(self):
        """Test error when channel is missing."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        self.manager._connection = mock_connection
        self.manager._channel = None

        with pytest.raises(Exception, match="RabbitMQ channel is not established"):
            self.manager.size()


class TestRabbitManagerGet:
    """Tests for the get() method."""

    def setup_method(self):
        """Setup for each test."""
        self.manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

    def test_get_message_success(self):
        """Test successful message retrieval."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()

        mock_method = Mock()
        mock_properties = Mock()
        mock_body = b"test message"

        mock_channel.basic_get.return_value = (
            mock_method,
            mock_properties,
            mock_body,
        )

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        message = self.manager.get()

        assert message == "test message"
        mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
        mock_channel.basic_get.assert_called_once_with(
            queue="test_queue", auto_ack=True
        )

    def test_get_empty_queue(self):
        """Test getting from empty queue."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.basic_get.return_value = (None, None, None)

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        message = self.manager.get()

        assert message is None

    @patch.object(RabbitManager, "open")
    def test_get_reconnects_when_closed(self, mock_open):
        """Test reconnection when connection is closed."""
        mock_connection = Mock()
        mock_connection.is_closed = True

        self.manager._connection = mock_connection

        # Simulate connection opening
        def side_effect():
            self.manager._connection = Mock()
            self.manager._connection.is_closed = False
            mock_channel = Mock()
            mock_channel.basic_get.return_value = (None, None, None)
            self.manager._channel = mock_channel

        mock_open.side_effect = side_effect

        message = self.manager.get()

        mock_open.assert_called_once()
        assert message is None

    def test_get_stream_lost_error(self):
        """Test connection loss error."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.basic_get.side_effect = StreamLostError()

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        with pytest.raises(StreamLostError):
            self.manager.get()


class TestRabbitManagerConsume:
    """Tests for the consume() method."""

    def setup_method(self):
        """Setup for each test."""
        self.manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

    def test_consume_message_success(self):
        """Test successful message retrieval via consume."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()

        mock_method = Mock()
        mock_properties = Mock()
        mock_body = b"consumed message"

        # Simulate generator with one message
        mock_channel.consume.return_value = iter(
            [(mock_method, mock_properties, mock_body)]
        )

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        message = self.manager.consume(timeout=10)

        assert message == "consumed message"
        mock_channel.basic_qos.assert_called_once_with(prefetch_count=1)
        mock_channel.consume.assert_called_once_with(
            queue="test_queue", inactivity_timeout=10, auto_ack=True
        )
        mock_channel.cancel.assert_called_once()

    def test_consume_timeout(self):
        """Test timeout while waiting for message."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()

        # Simulate timeout (method = None)
        mock_channel.consume.return_value = iter([(None, None, None)])

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        message = self.manager.consume(timeout=5)

        assert message is None
        mock_channel.cancel.assert_called_once()

    def test_consume_no_timeout(self):
        """Test waiting without timeout."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()

        mock_method = Mock()
        mock_properties = Mock()
        mock_body = b"message"

        mock_channel.consume.return_value = iter(
            [(mock_method, mock_properties, mock_body)]
        )

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        message = self.manager.consume(timeout=None)

        assert message == "message"
        mock_channel.consume.assert_called_once_with(
            queue="test_queue", inactivity_timeout=None, auto_ack=True
        )

    @patch.object(RabbitManager, "open")
    def test_consume_reconnects_when_closed(self, mock_open):
        """Test reconnection when connection is closed."""
        mock_connection = Mock()
        mock_connection.is_closed = True

        self.manager._connection = mock_connection

        # Simulate connection opening
        def side_effect():
            self.manager._connection = Mock()
            self.manager._connection.is_closed = False
            mock_channel = Mock()
            mock_channel.consume.return_value = iter([(None, None, None)])
            self.manager._channel = mock_channel

        mock_open.side_effect = side_effect

        message = self.manager.consume(timeout=1)

        mock_open.assert_called_once()
        assert message is None

    def test_consume_stream_lost_error(self):
        """Test connection loss error."""
        mock_connection = Mock()
        mock_connection.is_closed = False
        mock_channel = Mock()
        mock_channel.consume.side_effect = StreamLostError()

        self.manager._connection = mock_connection
        self.manager._channel = mock_channel

        with pytest.raises(StreamLostError):
            self.manager.consume()


class TestRabbitManagerIntegration:
    """Integration tests for checking method interactions."""

    @patch("rabbit_manager.pika.BlockingConnection")
    def test_full_workflow(self, mock_connection_class):
        """Test full workflow."""
        mock_connection = Mock()
        mock_channel = Mock()
        mock_connection.channel.return_value = mock_channel
        mock_connection.is_closed = False
        mock_connection_class.return_value = mock_connection

        # Setup mock for size
        mock_queue_declare_passive = Mock()
        mock_queue_declare_passive.method.message_count = 1

        # Setup mock for get
        mock_method = Mock()
        mock_properties = Mock()
        mock_body = b"test message"

        def queue_declare_side_effect(*args, **kwargs):
            if kwargs.get("passive"):
                return mock_queue_declare_passive
            return Mock()

        mock_channel.queue_declare.side_effect = queue_declare_side_effect
        mock_channel.basic_get.return_value = (mock_method, mock_properties, mock_body)

        manager = RabbitManager(
            host="localhost",
            port=5672,
            username="guest",
            password="guest",
            queue_name="test_queue",
        )

        # Open connection
        manager.open()
        assert manager._connection is not None
        assert manager._channel is not None

        # Add message
        result = manager.add("test message")
        assert result is True

        # Check size
        size = manager.size()
        assert size == 1

        # Get message
        message = manager.get()
        assert message == "test message"

        # Close connection
        manager.close()
        mock_connection.close.assert_called_once()
