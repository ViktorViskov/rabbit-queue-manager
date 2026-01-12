# RabbitMQ Queue Manager
<p>
  <img src=".imgs/pylogo.svg" height="40" />
  <img src=".imgs/plus.svg" height="40" />
  <img src=".imgs/rlogo.png" height="40" />
</p>

**RabbitMQ Queue Manager** is a simple, Python-based utility for managing RabbitMQ message queues. It provides an easy-to-use interface for sending, receiving, and processing messages from RabbitMQ queues, with built-in support for automatic reconnections and error handling.

**Key Features:**
 - Send messages to RabbitMQ queues.
 - Receive messages from queues with automatic acknowledgement.
 - Automatically reconnect on connection loss.
 - Easily handle multiple messages and batch operations.
 - Supports queue size inspection.
 - Iterable interface for message consumption.

This tool is ideal for developers looking to integrate RabbitMQ into their Python applications with minimal overhead.

# Requirements
- Python >= 3.10
- Pika (installed via project dependencies)

Install dependencies with `uv` (recommended):

```bash
uv sync --dev
```

Alternatively, using pip:

```bash
pip install -e ".[dev]"
```

Install Pika directly (if you only need the library):

Using uv:

```bash
uv add pika
```

Using pip:

```bash
pip install pika
```

# Quick Start (RabbitManager)

```python
from rabbit_manager import RabbitManager

# Create a manager for a RabbitMQ queue
manager = RabbitManager(
  host="localhost",
  port=5672,
  username="guest",
  password="guest",
  queue_name="my_queue",
  queue_durable=True,
  message_ttl_minutes=10,  # optional
  confirm_delivery=True,   # enable publisher confirms
)

# Use as a context manager to auto-open/close connection
with manager as q:
  # Add a message
  delivered = q.add("hello world")
  print("Delivered:", delivered)

  # Check queue size
  print("Queue size:", q.size())

  # Get one message (non-blocking)
  msg = q.get()
  print("Got:", msg)

  # Block and wait for a message (with optional timeout)
  msg2 = q.consume(timeout=5)
  print("Consumed:", msg2)

# Manual open/close usage
manager.open()
manager.add("another message")
print("Size:", manager.size())
print("Get:", manager.get())
manager.close()


Notes:
- The example assumes a local RabbitMQ instance on `localhost:5672` with `guest/guest` credentials.
- When `confirm_delivery=True`, publishing raises on delivery issues (e.g., unroutable or NACK).
- `message_ttl_minutes>0` sets per-queue message TTL.

# Testing

Before running tests, activate the virtual environment:

```bash
source .venv/bin/activate
```

Run the test suite:

```bash
pytest test.py -v
```

See more options in `TEST_README.md`.
