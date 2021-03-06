import datetime
import json
import threading
import time

import pika

import rabbit_producer
import rater

consumers = []


def init():
    tweets_consumer = TweetsConsumer('amqp://guest:guest@localhost:5672/%2F', 'rater', 'tweets', 'tweets')
    tweets_consumer.run()
    users_consumer = UsersConsumer('amqp://guest:guest@localhost:5672/%2F', 'rater', 'users', 'user')
    users_consumer.run()
    word_consumer = WordConsumer('amqp://guest:guest@localhost:5672/%2F', 'rater', 'word', 'user')
    word_consumer.run()


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

    def on_connection_closed(self, connection, reply_code):
        self._channel = None
        if self._closing:
            self._connection.ioloop.stop()
        else:
            time.sleep(5)
            self.run()

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

    def on_channel_closed(self, channel, exception):
        pass
        # self._connection.close()

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
        print("got tweet message %r %s" % (body, str(datetime.datetime.now())))
        try:
            data = json.loads(body.decode("utf-8"))
            if data.get("id") is not None:
                rater.process_tweet(data.get("id"))
            elif data.get("action") == 'end':
                rabbit_producer.publish(message=body)
        except Exception as e:
            print("got exception {}", e)
        print("processed message %r %s" % (body, str(datetime.datetime.now())))
        self.acknowledge_message(basic_deliver.delivery_tag)


class UsersConsumer(Consumer):
    def on_message(self, unused_channel, basic_deliver, properties, body):
        print("got user message %r" % body)
        try:
            if basic_deliver.redelivered:
                print('broken message, just ack')
                self.acknowledge_message(basic_deliver.delivery_tag)
            else:
                data = json.loads(body.decode("utf-8"))
                rater.process_user(data.get("username"))
                print("processed message %r" % body)
                self.acknowledge_message(basic_deliver.delivery_tag)
                rabbit_producer.publish(message=body)
        except Exception as e:
            print("got exception {}", e)


class WordConsumer(Consumer):
    def on_message(self, unused_channel, basic_deliver, properties, body):
        print("got word message %r" % body)
        try:
            if basic_deliver.redelivered:
                print('broken message, just ack')
                self.acknowledge_message(basic_deliver.delivery_tag)
            else:
                data = rater.calc_words()
                print("processed message %r" % body)
                self.acknowledge_message(basic_deliver.delivery_tag)
                rabbit_producer.publish(message=data, exchange='rater.out', key='user.word')
        except Exception as e:
            print("got exception {}", e)
