# Tests for RabbitManager

## Description

This file contains comprehensive unit tests for the `RabbitManager` class using pytest and unittest.mock.

## Installing Test Dependencies
Using uv (recommended):
```bash
uv sync --dev
```

Alternatively, using pip:
```bash
pip install -e ".[dev]"
```

## Running Tests

First, activate the virtual environment:
```bash
source .venv/bin/activate
```

Or using uv:
```bash
uv run pytest test.py -v
```

### Run all tests
```bash
pytest test.py -v
```

### Run specific test
```bash
pytest test.py::TestRabbitManagerAdd::test_add_success -v
```

## Test Structure

### TestRabbitManagerInit
Tests for verifying RabbitManager initialization with various parameters.

### TestRabbitManagerOpen
Tests for the `open()` method:
- Successful connection opening
- Opening with TTL
- Opening with/without confirm_delivery
- Authentication error handling

### TestRabbitManagerClose
Tests for the `close()` method:
- Closing active connection
- Closing already closed connection
- Closing without connection

### TestRabbitManagerContextManager
Tests for using RabbitManager as a context manager.

### TestRabbitManagerAdd
Tests for the `add()` method:
- Successful message addition
- Automatic reconnection
- Handling various errors (UnroutableError, NackError, StreamLostError)
- Priority message publishing when `max_priority > 0`

### TestRabbitManagerSize
Tests for the `size()` method:
- Getting queue size
- Reconnection when needed

### TestRabbitManagerGet
Tests for the `get()` method:
- Getting message from queue
- Handling empty queue
- Reconnection when needed

### TestRabbitManagerConsume
Tests for the `consume()` method:
- Blocking wait for message
- Timeout while waiting
- Waiting without timeout

### TestRabbitManagerIntegration
Integration tests for verifying full workflow.

## Code Coverage

Tests cover:
- ✅ Class initialization
- ✅ Opening and closing connections
- ✅ Adding messages
- ✅ Getting messages (get and consume)
- ✅ Checking queue size
- ✅ Context manager
- ✅ Error handling
- ✅ Automatic reconnection

## Example Output

```
test.py::TestRabbitManagerInit::test_init_default_values PASSED
test.py::TestRabbitManagerInit::test_init_custom_values PASSED
test.py::TestRabbitManagerOpen::test_open_success PASSED
test.py::TestRabbitManagerOpen::test_open_with_ttl PASSED
...
============================== 30 passed in 0.15s ==============================
```

## Additional Features

### Run tests with different verbosity levels
```bash
# Quiet mode
pytest test.py -q

# Verbose mode
pytest test.py -vv

# Show print statements
pytest test.py -s
```

### Run only failed tests
```bash
pytest test.py --lf
```
