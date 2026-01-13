import pika

from .manager import RabbitManager

# Expose pika so test patches like `rabbit_manager.pika.BlockingConnection` work
__all__ = ["RabbitManager", "pika"]
__version__ = "0.1.3"