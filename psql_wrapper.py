import psycopg2


def connection():
    conn = psycopg2.connect(dbname='gakachu', user='gakachu',
                            password='123123123', host='18.222.72.126')
    return conn


def get_cursor():
    return connection().cursor()


def get_user_tweets(user_id):
    cursor = get_cursor()
    cursor.execute('select * from tweets where username = %s', [str(user_id)])
    results = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results


def get_all_tweets():
    cursor = get_cursor()
    cursor.execute('select * from tweets')
    results = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results


def get_users_rates(users):
    cursor = get_cursor()
    cursor.execute('select * from user_rate where username IN %(users)s', {'users': tuple(users)})
    results = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results


def get_tweet(tweet_id):
    cursor = get_cursor()
    cursor.execute('select * from tweets where id = %s', [str(tweet_id)])
    results = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results[0]


def insert_user_data(username, date, rate):
    conn = connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO user_data (username, time_start, rate) VALUES(%s, %s, %s);', (username, date, rate))
    conn.commit()
    cursor.close()
    conn.close()


def insert_user_rate(username, rate):
    conn = connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO user_rate (username, rate) VALUES (%s, %s);', (username, rate))
    conn.commit()
    cursor.close()
    conn.close()
    conn = connection()
    cursor = conn.cursor()
    cursor.execute('INSERT INTO tmp_user_rate (username, rate) VALUES (%s, %s);', (username, rate))
    conn.commit()
    cursor.close()
    conn.close()


def save(tweet):
    conn = connection()
    cursor = conn.cursor()
    tweet_id = str(tweet["id"])
    rate = str(tweet["emotion"])
    cursor.execute('update tweets set emotion = %s where id = %s;', (rate, tweet_id))
    conn.commit()
    cursor.close()
    conn.close()


def drop_user_data(username):
    conn = connection()
    cursor = conn.cursor()
    cursor.execute('delete from user_data where username = %s;', [username])
    conn.commit()
    cursor.close()
    conn.close()


def drop_user_rate(username):
    conn = connection()
    cursor = conn.cursor()
    cursor.execute('delete from user_rate where username = %s;', [username])
    conn.commit()
    cursor.close()
    conn.close()
    conn = connection()
    cursor = conn.cursor()
    cursor.execute('delete from tmp_user_rate where username = %s;', [username])
    conn.commit()
    cursor.close()
    conn.close()


def load_words(words):
    conn = connection()
    cursor = conn.cursor()
    for p in words.keys():
        cursor.execute('INSERT INTO words (word, rate) VALUES(%s, %s);', (p, words[p]))
    conn.commit()
    cursor.close()
    conn.close()


def get_rate_by_word(word):
    conn = connection()
    cursor = conn.cursor()
    cursor.execute('select * from words where word = %s;', [word])
    results = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    if len(results) > 0:
        return results[0]["rate"]
    return None


def get_words():
    conn = connection()
    cursor = conn.cursor()
    cursor.execute('select * from words;')
    results = []
    columns = [column[0] for column in cursor.description]
    for row in cursor.fetchall():
        results.append(dict(zip(columns, row)))
    return results
