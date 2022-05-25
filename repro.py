import uuid

import pika
from locust import User

from utils.time_measure import decorate_time


class Rabbit:
    def __init__(self, host, exchange_name, routing_key, virtual_host):
        self.virtual_host = virtual_host
        self.host = host
        self.routing_key = routing_key
        self.exchange_name = exchange_name
        self.connection = None
        self.channels = []

    def connect(self) -> None:
        """Connect to Rabbit and create a pool of channels"""
        credentials = pika.PlainCredentials("admin", "admin")
        parameters = pika.ConnectionParameters(
            host=self.host,
            credentials=credentials,
            virtual_host=self.virtual_host,
            port=35672,
            channel_max=256,
            heartbeat=60,
        )
        connection = pika.BlockingConnection(parameters)
        self.connection = connection
        for _ in range(10):
            self.channels.append(connection.channel())

    def close(self) -> None:
        """Close all channels and close connection"""
        for channel in self.channels:
            channel.close()
        self.connection.close()

    @decorate_time
    def publish(self, body="") -> None:
        """
        Publish a message with param <body> in open channel. If channel is_closed or number of channels not enough
        created a new one.
        :param body: str
        """
        try:
            channel = self.channels.pop(0)
            if channel.is_closed():
                channel = self.connection.channel()
        except IndexError as e:
            print(repr(e))
            print("List is empty")
            channel = self.connection.channel()
        channel.basic_publish(
            exchange=self.exchange_name,
            routing_key=self.routing_key,
            body=body,
            properties=pika.BasicProperties(
                content_type="application/json",
                delivery_mode=1,
                content_encoding="UTF-8",
                headers={"X-Flow-ID": str(uuid.uuid4())},
            ),
        )
        self.channels.append(channel)


class CustomRabbitLocust(User):
    abstract = True

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = Rabbit(
            self.host, self.exchange_name, self.routing_key, self.virtual_host
        )
