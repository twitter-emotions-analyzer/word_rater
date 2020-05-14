import pika

connection = None
channel = None


def init():
    global connection
    global channel
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()

    channel.queue_bind(exchange='rater',
                       queue='rater.out')


def publish(message):
    global channel
    channel.basic_publish(exchange='rater', routing_key='rater.out', body=message)
