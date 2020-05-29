import json
import pika
import rater
import threading
import rabbit_producer

consumers = []


def init():
    # print("init rabbit")
    # connection = pika.BlockingConnection(
    #     pika.ConnectionParameters(host='localhost'))
    # channel = connection.channel()
    # channel.exchange_declare(exchange='rater', exchange_type='topic', durable=True)
    #
    # channel.queue_bind(exchange='rater', queue="rater")
    #
    # channel.basic_consume(
    #     queue="tweets", on_message_callback=callback, auto_ack=True)
    #
    # channel.start_consuming()
    tweets_consumer = TweetsConsumer('amqp://guest:guest@localhost:5672/%2F', 'rater', 'tweets', 'tweets')
    tweets_consumer.run()
    users_consumer = UsersConsumer('amqp://guest:guest@localhost:5672/%2F', 'rater', 'users', 'user')
    users_consumer.run()


def callback(ch, method, properties, body):
    print("got message %r" % body)
    data = json.loads(body.decode("utf-8"))
    rater.process_user(data.get("id"))
    print("processed message %r" % body)


class Consumer(object):
    EXCHANGE = 'rater'
    EXCHANGE_TYPE = 'topic'
    QUEUE = 'tweets'
    ROUTING_KEY = 'tweets'

    def __init__(self, amqp_url, exchange, queue, routing_key):
        self.EXCHANGE = exchange
        self.QUEUE = queue
        self.ROUTING_KEY = routing_key
        self._connection = None
        self._channel = None
        self._closing = False
        self._consumer_tag = None
        self._url = amqp_url

    def connect(self):
        return pika.SelectConnection(pika.URLParameters(self._url),
                                     self.on_connection_open)

    def on_connection_open(self, unused_connection):
        self.add_on_connection_close_callback()
        self.open_channel()

    def add_on_connection_close_callback(self):
        self._connection.add_on_close_callback(self.on_connection_closed)

    def on_connection_closed(self, connection, reply_code, reply_text):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            self._connection.add_timeout(5, self.reconnect)

    def reconnect(self):
        self._connection.ioloop.stop()

        if not self._closing:
            self._connection = self.connect()
            self._connection.ioloop.start()

    def open_channel(self):
        self._connection.channel(on_open_callback=self.on_channel_open)

    def on_channel_open(self, channel):
        self._channel = channel
        self.add_on_channel_close_callback()
        self.setup_exchange(self.EXCHANGE)

    def add_on_channel_close_callback(self):
        self._channel.add_on_close_callback(self.on_channel_closed)

    def on_channel_closed(self, channel, reply_code, reply_text):
        self._connection.close()

    def setup_exchange(self, exchange_name):
        self._channel.exchange_declare(callback=self.on_exchange_declareok,
                                       exchange=exchange_name,
                                       exchange_type=self.EXCHANGE_TYPE,
                                       durable=True)

    def on_exchange_declareok(self, unused_frame):
        self.setup_queue(self.QUEUE)

    def setup_queue(self, queue_name):
        self._channel.queue_declare(callback=self.on_queue_declareok, queue=queue_name, durable=True)

    def on_queue_declareok(self, method_frame):
        self._channel.queue_bind(callback=self.on_bindok, queue=self.QUEUE,
                                 exchange=self.EXCHANGE, routing_key=self.ROUTING_KEY)

    def on_bindok(self, _unused_frame):
        self.start_consuming()

    def start_consuming(self):
        self.add_on_cancel_callback()
        self._consumer_tag = self._channel.basic_consume(on_message_callback=self.on_message,
                                                         queue=self.QUEUE)

    def add_on_cancel_callback(self):
        self._channel.add_on_cancel_callback(self.on_consumer_cancelled)

    def on_consumer_cancelled(self, method_frame):
        if self._channel:
            self._channel.close()

    def on_message(self, unused_channel, basic_deliver, properties, body):
        pass

    def acknowledge_message(self, delivery_tag):
        self._channel.basic_ack(delivery_tag)

    def stop_consuming(self):
        if self._channel:
            self._channel.basic_cancel(callback=self.on_cancelok, consumer_tag=self._consumer_tag)

    def on_cancelok(self, unused_frame):
        self.close_channel()

    def close_channel(self):
        self._channel.close()

    def run(self):
        self._connection = self.connect()
        thread = threading.Thread(target=self._connection.ioloop.start)
        thread.setDaemon(True)
        thread.start()

    def stop(self):
        self._closing = True
        self.stop_consuming()
        self._connection.ioloop.start()

    def close_connection(self):
        self._connection.close()


class TweetsConsumer(Consumer):
    def on_message(self, unused_channel, basic_deliver, properties, body):
        print("got message %r" % body)
        data = json.loads(body.decode("utf-8"))
        rater.process_tweet(data.get("id"))
        print("processed message %r" % body)
        self.acknowledge_message(basic_deliver.delivery_tag)


class UsersConsumer(Consumer):
    def on_message(self, unused_channel, basic_deliver, properties, body):
        print("got message %r" % body)
        data = json.loads(body.decode("utf-8"))
        rater.process_user(data.get("id"))
        print("processed message %r" % body)
        self.acknowledge_message(basic_deliver.delivery_tag)
        rabbit_producer.publish(message=body)