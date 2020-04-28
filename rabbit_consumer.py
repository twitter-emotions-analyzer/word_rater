import json
import pika
import rater


def init():
    print("init rabbit")
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange='rater', exchange_type='direct', durable=True)
    result = channel.queue_declare(queue='', exclusive=True)

    channel.queue_bind(exchange='rater', queue="rater")

    channel.basic_consume(
        queue="rater", on_message_callback=callback, auto_ack=True)

    channel.start_consuming()


def callback(ch, method, properties, body):
    print("got message %r" % body)
    data = json.loads(body.decode("utf-8"))
    rate, words = rater.rate_string(rater.get_data(), data.get("string"), 1, False)
    print("rate: {}, words {}".format(str(rate), str(words)))
