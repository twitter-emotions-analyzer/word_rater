import json
import psql_wrapper
import rater
import requests

rate_data = {}
self_made_data = {}


# reads words dictionary into python dictionary
def get_data():
    global rate_data
    if not rate_data:
        with open("en_words.json") as file:
            rate_data = json.load(file)
    return rate_data


def load_words_to_db():
    psql_wrapper.load_words(get_data())


# get dictionary made via OD
def get_self_made_data():
    global self_made_data
    if not self_made_data:
        with open("made_dictionary.json") as file:
            self_made_data = json.load(file)
    return self_made_data


def store_self_made_data(new_data):
    global rate_data
    global self_made_data
    rate_data.update(new_data)
    with open("en_words.json", "w") as output:
        json.dump(rate_data, output)
    for w in new_data.keys():
        if w in self_made_data.keys():
            self_made_data.pop(w, None)
    with open("made_dictionary.json", "w") as output:
        json.dump(self_made_data, output)


def get_data_from_od(word):
    global self_made_data
    if word in self_made_data:
        return self_made_data[word]
    rate = rater.rate_via_od(get_data(), word)
    self_made_data.update({word: rate})
    return rate


def get_user_tweets(user_id):
    return psql_wrapper.get_user_tweets(user_id)


def get_tweet(tweet_id):
    return psql_wrapper.get_tweet(tweet_id)


def get_all_tweets():
    return psql_wrapper.get_all_tweets()


def get_users_rates(users):
    rates = psql_wrapper.get_users_rates(users)
    result = {}
    for r in rates:
        if r["username"] not in result.keys():
            result[r["username"]] = r["rate"]
    return result


def save(tweet):
    psql_wrapper.save(tweet)


def get_neural_rate(rates):
    return requests.post(url="http://localhost:1489/calc", json=json.dumps({"data": rates}))


def insert_user_data(username, date, rate):
    psql_wrapper.insert_user_data(username, date, rate)


def insert_user_rate(username, rate):
    psql_wrapper.insert_user_rate(username, rate)


def drop_user_data(username):
    psql_wrapper.drop_user_data(username)
    psql_wrapper.drop_user_rate(username)
