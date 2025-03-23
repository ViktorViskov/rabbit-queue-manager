from rabbit_queue_manager import RabbitQueueManager


# Create an object to manage the RabbitMQ queue
queue = RabbitQueueManager("some_queue")

# Add a single message to the queue
queue.add_to_queue("some message")

# Add a list of messages to the queue
queue.add_list_to_queue(["message1", "message2", "message3"])

# Print the current size of the queue
print(f"Queue size: {queue.get_size()}")

# Use the built-in len() function to check the number of messages
print(f"Queue size using len(): {len(queue)}")

# Retrieve the next message from the queue
item = queue.get_next()
print(f"Received message: {item}")

# The queue size decreases after receiving a message
print(f"Queue size after receiving one message: {len(queue)}")

# Iterate over the remaining messages in the queue
print("Iterating over the remaining messages:")
for item in queue:
    print(f"Received: {item}")

# Check the size of the queue after iteration
print(f"Queue size after iteration: {len(queue)}")

# Try to retrieve a message from the empty queue
print(f"Next message (should be None): {queue.get_next()}")