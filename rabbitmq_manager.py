from pika.adapters import TornadoConnection
import pika

import logging

from singleton import Singleton


AMQP_URL = 'amqp://guest:guest@localhost:5672/%2F'

LOG_FORMAT = ('%(levelname) -10s %(asctime)s %(name) -30s %(funcName) '
              '-35s %(lineno) -5d: %(message)s')
logger = logging.getLogger(__name__)


class PikaConnection(object):
    __metaclass__ = Singleton

    QUEUE = 'test'
    EXCHANGE = ''
    PUBLISH_INTERVAL = 1

    def __init__(self, io_loop=None, amqp_url=AMQP_URL):
        print "__init__"
        self.io_loop = io_loop
        self.url = amqp_url

        self.is_connected = False
        self.is_connecting = False

        self.connection = None
        self.channel = None

        self.listeners = set([])

    def connect(self):
        if self.is_connecting:
            logger.info("PikaConnection: Already connecting to RabbitMQ")
            return

        logger.info("PikaConnection: Connecting to RabbitMQ")
        self.connecting = True

        self.connection = TornadoConnection(
            pika.URLParameters(self.url),
            on_open_callback=self.on_connected)

        self.connection.add_on_close_callback(self.on_closed)

    def on_connected(self, connection):
        logger.info("PikaConnection: connected to RabbitMQ")
        self.connected = True
        self.connection = connection
        self.connection.channel(self.on_channel_open)

    def on_closed(self, connection):
        logger.info("PikaConnection: connection closed")
        if self.io_loop is not None:
            self.io_loop.stop()

    def on_channel_open(self, channel):
        print "on_channel_open"
        print channel
        self.channel = channel
        self.channel.add_on_close_callback(self.on_channel_closed)
        self.channel.queue_declare(queue=self.QUEUE,
                                   callback=self.on_queue_declared)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self.connection.close()

    def on_queue_declared(self, frame):
        print "subscribe frame:", frame
        self.channel.basic_consume(self.on_message, self.QUEUE)

    def on_message(self, channel, method, header, body):
        logger.info(body)
        for listener in self.listeners:
            listener.emit(body)

    def add_event_listener(self, listener):
        self.listeners.add(listener)

    def publish_message(self, *args, **kwargs):
        if 'message' in kwargs:
            self.channel.basic_publish(exchange=self.EXCHANGE,
                                       routing_key=self.QUEUE,
                                       body=kwargs['message'])
