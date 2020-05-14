import psycopg2


def connection():
    conn = psycopg2.connect(dbname='gakachu', user='postgres',
                            password='mypassword', host='localhost')

    return conn


def get_user_tweets(user_id):
    cursor = connection().cursor()
    cursor.execute('select * from tweets where user_id = %s', user_id)
    return cursor.fetchall()