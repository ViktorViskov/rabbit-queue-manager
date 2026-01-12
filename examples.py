"""
RabbitMQ Manager - Comprehensive Usage Examples

This module demonstrates the main features and usage patterns of the RabbitManager library.
It covers basic queue operations, context manager usage, batch processing, and error handling.

Prerequisites:
    - RabbitMQ server running on localhost:5672 (or adjust host/port)
    - Default credentials: username='guest', password='guest'
    - Python >= 3.10
    - pika library installed
"""

from rabbit_manager import RabbitManager
import logging
import time

# Configure logging to see debug messages
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ============================================================================
# Example 1: Basic Queue Operations with Context Manager
# ============================================================================
def example_basic_context_manager():
    """
    Demonstrates the basic usage of RabbitManager with a context manager.

    The context manager (with statement) automatically handles connection
    opening and closing, ensuring clean resource management.
    """
    print("\n" + "="*70)
    print("EXAMPLE 1: Basic Queue Operations with Context Manager")
    print("="*70)

    # Create a manager instance with configuration
    manager = RabbitManager(
        "example_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
        host="localhost",  # optional, defaults to "localhost"
        port=5672,  # optional, defaults to 5672
        queue_durable=True,  # Queue persists after server restart
        message_ttl_minutes=0,  # No message expiration (0 = infinite)
        confirm_delivery=True,  # Enable publisher confirms (raises exceptions on failure)
    )

    # Use context manager for automatic connection management
    with manager as q:
        # Add (publish) a message to the queue
        success = q.add("Hello, RabbitMQ!")
        print(f"Message sent successfully: {success}")

        # Check the current queue size
        queue_size = q.size()
        print(f"Messages in queue: {queue_size}")

        # Get a message (non-blocking, returns None if empty)
        message = q.get()
        print(f"Retrieved message: {message}")

        # Queue should be empty now
        final_size = q.size()
        print(f"Queue size after retrieval: {final_size}")


# ============================================================================
# Example 2: Manual Connection Management
# ============================================================================
def example_manual_connection():
    """
    Demonstrates manual connection open/close without context manager.

    Useful for scenarios where you need more control over connection lifecycle
    or want to reuse a single connection for multiple operations.
    """
    print("\n" + "="*70)
    print("EXAMPLE 2: Manual Connection Management")
    print("="*70)

    manager = RabbitManager(
        "manual_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
    )

    try:
        # Explicitly open the connection
        manager.open()
        print("Connection opened successfully")

        # Send multiple messages
        for i in range(3):
            manager.add(f"Message {i+1}")
        print("Sent 3 messages")

        # Check size
        print(f"Queue size: {manager.size()}")

    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Always close the connection when done
        manager.close()
        print("Connection closed")


# ============================================================================
# Example 3: Publishing with Delivery Confirmation
# ============================================================================
def example_publish_with_confirmation():
    """
    Demonstrates publishing messages with delivery confirmation enabled.

    When confirm_delivery=True, the publisher waits for acknowledgement from
    the broker. Exceptions are raised if the message cannot be delivered.
    """
    print("\n" + "="*70)
    print("EXAMPLE 3: Publishing with Delivery Confirmation")
    print("="*70)

    manager = RabbitManager(
        "confirmed_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
        confirm_delivery=True,  # Enable confirmation mode
    )

    with manager as q:
        messages = [
            "Order #001: 5 units of Product A",
            "Order #002: 3 units of Product B",
            "Order #003: 10 units of Product C",
        ]

        delivered_count = 0
        for msg in messages:
            try:
                if q.add(msg):
                    delivered_count += 1
                    print(f"✓ Delivered: {msg}")
                else:
                    print(f"✗ Failed: {msg}")
            except Exception as e:
                print(f"✗ Error publishing '{msg}': {e}")

        print(f"\nSuccessfully delivered {delivered_count}/{len(messages)} messages")


# ============================================================================
# Example 4: Consuming Messages (Blocking with Timeout)
# ============================================================================
def example_consume_with_timeout():
    """
    Demonstrates the consume() method which blocks and waits for messages.

    The timeout parameter specifies how long to wait before returning None
    if no message arrives.
    """
    print("\n" + "="*70)
    print("EXAMPLE 4: Consuming Messages with Timeout")
    print("="*70)

    manager = RabbitManager(
        "consume_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
    )

    with manager as q:
        # Pre-load some messages
        print("Adding messages to queue...")
        q.add("Message 1")
        q.add("Message 2")

        # Consume messages with 5 second timeout
        print("Waiting for messages (timeout=5 seconds)...")

        msg1 = q.consume(timeout=5)
        if msg1:
            print(f"Received: {msg1}")

        msg2 = q.consume(timeout=5)
        if msg2:
            print(f"Received: {msg2}")

        # Try to consume when queue is empty
        print("Waiting for another message (queue is empty)...")
        msg3 = q.consume(timeout=2)
        if msg3 is None:
            print("No message received (timeout expired)")


# ============================================================================
# Example 5: Batch Processing Pattern
# ============================================================================
def example_batch_processing():
    """
    Demonstrates a typical batch processing pattern:
    1. Publish a batch of work items
    2. Process them one by one
    3. Track progress
    """
    print("\n" + "="*70)
    print("EXAMPLE 5: Batch Processing Pattern")
    print("="*70)

    manager = RabbitManager(
        "batch_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
    )

    with manager as q:
        # Publisher: Add multiple work items
        print("Publisher: Adding work items...")
        work_items = ["task_1", "task_2", "task_3", "task_4", "task_5"]
        for item in work_items:
            q.add(item)
        print(f"Added {len(work_items)} work items")

        # Get initial queue size
        total = q.size()
        print(f"Total items in queue: {total}\n")

        # Consumer: Process all items
        print("Consumer: Processing items...")
        processed = 0
        while True:
            message = q.get()
            if message is None:
                break

            processed += 1
            remaining = q.size()
            print(f"  [{processed}/{total}] Processing: {message} (remaining: {remaining})")
            # Simulate processing time
            time.sleep(0.1)

        print(f"\nCompleted processing {processed} items")


# ============================================================================
# Example 6: Message TTL (Time To Live)
# ============================================================================
def example_message_ttl():
    """
    Demonstrates message TTL (Time To Live) functionality.

    When message_ttl_minutes is set to a positive value, messages in the queue
    automatically expire and are deleted after the specified time.
    """
    print("\n" + "="*70)
    print("EXAMPLE 6: Message TTL (Time To Live)")
    print("="*70)

    manager = RabbitManager(
        "ttl_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
        message_ttl_minutes=1,  # Messages expire after 1 minute
    )

    with manager as q:
        # Send some messages
        print("Sending messages with 1-minute TTL...")
        for i in range(3):
            q.add(f"Temporary message {i+1}")

        print(f"Queue size: {q.size()}")
        print("These messages will automatically expire in 1 minute")
        print("(TTL is configured at the queue level, not per message)")


# ============================================================================
# Example 7: Queue Durability
# ============================================================================
def example_queue_durability():
    """
    Demonstrates queue durability settings.

    When queue_durable=True:
    - The queue persists even if the RabbitMQ broker restarts
    - Messages in the queue are preserved

    When queue_durable=False:
    - The queue is deleted when the broker restarts
    - Useful for temporary queues
    """
    print("\n" + "="*70)
    print("EXAMPLE 7: Queue Durability")
    print("="*70)

    # Durable queue (persists after restart)
    durable_manager = RabbitManager(
        "durable_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
        queue_durable=True,  # Queue survives broker restart
    )

    # Non-durable queue (temporary)
    temp_manager = RabbitManager(
        "temp_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
        queue_durable=False,  # Queue is deleted on broker restart
    )

    print("Queue 'durable_queue': Will persist after RabbitMQ restarts")
    print("Queue 'temp_queue': Will be deleted if RabbitMQ restarts")


# ============================================================================
# Example 8: Error Handling
# ============================================================================
def example_error_handling():
    """
    Demonstrates proper error handling when working with RabbitMQ.

    Common errors:
    - ProbableAuthenticationError: Wrong credentials
    - UnroutableError: Message cannot be routed to queue
    - NackError: Broker rejected the message
    - StreamLostError: Connection lost
    """
    print("\n" + "="*70)
    print("EXAMPLE 8: Error Handling")
    print("="*70)

    # Example 1: Handling authentication errors
    print("\nExample 1: Authentication Error Handling")
    print("-" * 50)
    try:
        bad_manager = RabbitManager(
            "test_queue",  # queue_name (positional argument)
            username="wrong_user",
            password="wrong_pass",
        )
        bad_manager.open()
    except Exception as e:
        print(f"✓ Caught expected error: {type(e).__name__}")
        print(f"  Message: {str(e)[:60]}...")

    # Example 2: Handling delivery errors
    print("\nExample 2: Message Delivery Error Handling")
    print("-" * 50)
    try:
        manager = RabbitManager(
            "good_queue",  # queue_name (positional argument)
            username="guest",
            password="guest",
            confirm_delivery=True,
        )
        with manager as q:
            # This should succeed
            result = q.add("Valid message")
            print(f"✓ Message delivery: {result}")
    except Exception as e:
        print(f"✗ Error: {e}")


# ============================================================================
# Example 9: Producer-Consumer Pattern
# ============================================================================
def example_producer_consumer():
    """
    Demonstrates a typical producer-consumer pattern.

    - Producer: Generates and sends work items
    - Consumer: Processes items from the queue
    """
    print("\n" + "="*70)
    print("EXAMPLE 9: Producer-Consumer Pattern")
    print("="*70)

    manager = RabbitManager(
        "producer_consumer_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
    )

    # Producer phase
    print("PRODUCER PHASE: Generating data...")
    with manager as q:
        data_items = [
            {"id": 1, "data": "Record 1"},
            {"id": 2, "data": "Record 2"},
            {"id": 3, "data": "Record 3"},
        ]

        for item in data_items:
            import json
            q.add(json.dumps(item))
        print(f"Produced {len(data_items)} items")

    # Consumer phase
    print("\nCONSUMER PHASE: Processing data...")
    with manager as q:
        import json
        count = 0
        while True:
            message = q.get()
            if message is None:
                break

            item = json.loads(message)
            count += 1
            print(f"  Consumed: ID={item['id']}, Data={item['data']}")

        print(f"Consumed {count} items")


# ============================================================================
# Example 10: Connection Resilience
# ============================================================================
def example_connection_resilience():
    """
    Demonstrates automatic reconnection on connection loss.

    RabbitManager automatically detects when the connection is closed
    and attempts to reconnect when methods like add() or get() are called.
    """
    print("\n" + "="*70)
    print("EXAMPLE 10: Connection Resilience")
    print("="*70)

    manager = RabbitManager(
        "resilience_queue",  # queue_name (positional argument)
        username="guest",
        password="guest",
    )

    with manager as q:
        print("Connection established")

        # Send a message
        q.add("Message 1")
        print("Sent message 1")

        # If connection were lost here (e.g., broker restart),
        # the next operation would trigger automatic reconnection
        print("\nSimulating automatic reconnection on next operation...")

        q.add("Message 2")
        print("Sent message 2 (reconnected automatically if needed)")

        # Retrieve messages
        msg1 = q.get()
        msg2 = q.get()
        print(f"Retrieved: {msg1}")
        print(f"Retrieved: {msg2}")


# ============================================================================
# Main Function - Run All Examples
# ============================================================================
def main():
    """
    Run all examples demonstrating RabbitManager usage.

    To run individual examples, modify this function or call them directly.
    """
    print("\n")
    print("█" * 70)
    print("█" + " " * 68 + "█")
    print("█" + "  RabbitMQ Manager - Comprehensive Usage Examples".center(68) + "█")
    print("█" + " " * 68 + "█")
    print("█" * 70)

    # List of all examples
    examples = [
        ("Basic Context Manager", example_basic_context_manager),
        ("Manual Connection", example_manual_connection),
        ("Publish with Confirmation", example_publish_with_confirmation),
        ("Consume with Timeout", example_consume_with_timeout),
        ("Batch Processing", example_batch_processing),
        ("Message TTL", example_message_ttl),
        ("Queue Durability", example_queue_durability),
        ("Error Handling", example_error_handling),
        ("Producer-Consumer", example_producer_consumer),
        ("Connection Resilience", example_connection_resilience),
    ]

    print("\nAvailable examples:")
    for i, (name, _) in enumerate(examples, 1):
        print(f"  {i:2d}. {name}")

    print("\n" + "="*70)
    print("Running all examples...")
    print("="*70)

    for name, example_func in examples:
        try:
            example_func()
        except Exception as e:
            print(f"\n✗ Example '{name}' failed: {e}")
            logger.exception(f"Error in {name}")
        time.sleep(0.5)

    print("\n" + "█" * 70)
    print("█" + "  All examples completed!".center(68) + "█")
    print("█" * 70 + "\n")


if __name__ == "__main__":
    main()
