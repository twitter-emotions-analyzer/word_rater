import threading
import time
from functools import reduce
from multiprocessing import Pool
import json

from flask import Flask
from flask import request

import data_wrapper
import dictionary
import rabbit_consumer

app = Flask("rater")


# reads dataset into list of strings
def prepare_content():
    f = open("testData.txt", "r")
    content = f.read()
    return content.split("\n")


# checks strings equality ignoring case and using levenshtein distance
def are_strings_equal(s1, s2, d=1):
    s1 = s1.lower()
    s2 = s2.lower()
    if s1 == s2:
        return True
    if distance(s1, s2) <= d:
        return True
    return False


# calculates levenshtein distance between two words
def distance(a, b):
    n, m = len(a), len(b)
    if n > m:
        a, b = b, a
        n, m = m, n
    current_row = range(n + 1)
    for i in range(1, m + 1):
        previous_row, current_row = current_row, [i] + [0] * n
        for j in range(1, n + 1):
            add, delete, change = previous_row[j] + 1, current_row[j - 1] + 1, previous_row[j - 1]
            if a[j - 1] != b[i - 1]:
                change += 1
            current_row[j] = min(add, delete, change)
    return current_row[n]


# split provided string using punctuation marks
def string_split(s):
    result = s.split(" ")
    result = split_list(result, ".")
    result = split_list(result, ",")
    result = split_list(result, "?")
    result = split_list(result, ":")
    result = split_list(result, "!")
    result = split_list(result, ";")
    result = split_list(result, ")")
    result = split_list(result, "(")
    return result


# splits all string in list with separator
def split_list(l, sep):
    tmp = []
    for t in l:
        result = t.split(sep)
        for k in result:
            if k != "" and k != " ":
                tmp.append(k)
    return tmp


def reducer(ch1, ch2):
    return ch1 + ch2


def rate_string(data, string_to_check, d=1, use_od=False):
    rate = 0
    out = []
    # iterating over all words in provided string
    words = string_split(string_to_check)
    pool = Pool(8)
    mapped = pool.map(rate_word, words, 16)
    reduced = reduce(reducer, mapped)
    if len(words) > 0:
        reduced /= len(words)
    pool.close()
    return reduced, None
    # for l in words:
    #     print("word:", l)
    #     word_rate = rate_word(l, d)
    #     if word_rate == 0.0:  # if nothing has been found this word should be highlighted
    #         # first - let's check oxford dictionary
    #         if use_od:  # if it's original string, not the one from OD
    #             word_rate = data_wrapper.get_data_from_od(l)
    #             dictionary.store_data(l, word_rate)
    #         else:
    #             out.append(l)
    #     rate += word_rate
    # # normalizing string's rate
    # if len(words) > 0:
    #     rate /= max((len(words) - len(out)), 1)
    # return rate, out


def rate_word(word, d=0):
    data = data_wrapper.get_data()
    rate = 0.0
    found_words = find_appropriate_words(word, data, d)
    if len(found_words) > 0:
        found = False
        for w in found_words.keys():
            if are_strings_equal(w, word, 0):  # first we need to check if the same word has been found
                rate += found_words[w]
                found = True
                break
        if not found:  # otherwise let's normalize all found odds
            tmp_rate = 0
            for w in found_words.keys():
                tmp_rate += found_words[w]
            tmp_rate /= len(found_words)
            rate += tmp_rate
    return rate


def find_appropriate_words(word, data, d):
    found_words = {}  # dict of words that might be same as one in string
    for p in data.keys():
        if are_strings_equal(word, p, d):
            found_words.update({p: data[p]})
    return found_words


def rate_via_od(data, word):
    od_rate, od_data = dictionary.get_data_from_od(word)
    if od_rate is not None:
        return od_rate
    word_rate = 0
    count = 0
    # get all definitions for word, rate them and normalize
    if "results" in od_data:
        for p in od_data["results"][0]["lexicalEntries"]:
            for m in p["entries"][0]["senses"]:
                if "definitions" in m:
                    result_string = m["definitions"][0]
                    rate, missing_words = rate_string(data, result_string, 0)
                    word_rate += rate
                    count += 1
    if count != 0:
        word_rate /= count
    return word_rate


def process_user(user_id):
    tweets = data_wrapper.get_user_tweets(user_id)
    while not all_tweets_rated(tweets):
        time.sleep(10)
        tweets = data_wrapper.get_user_tweets(user_id)

    data_wrapper.drop_user_data(user_id)
    user_rate = 0

    data = {}

    for t in tweets:
        date_tmp = t["date"]
        if date_tmp in data.keys():
            rate, count = data[date_tmp]
            rate += float(t["emotion"])
            count += 1
            data[date_tmp] = (rate, count)
        else:
            data[date_tmp] = (float(t["emotion"]), 1)
        user_rate += float(t["emotion"])

    rates = []
    for p in data.keys():
        rate, count = data[p]
        rates.append(rate)
        if count < 0:
            rate /= count
        else:
            rate = 0
        data_wrapper.insert_user_data(user_id, p, rate)
    if len(tweets) > 0:
        user_rate = data_wrapper.get_neural_rate(rates).json()
        data_wrapper.insert_user_rate(user_id, user_rate)


def all_tweets_rated(tweets):
    for t in tweets:
        if t["emotion"] is None:
            return False
    return True


def process_tweet(tweet_id):
    tweet = data_wrapper.get_tweet(tweet_id)
    if tweet["emotion"] is None:
        rate, missing_words = rate_string(data_wrapper.get_data(), tweet["tweet_text"], 0, False)
        tweet["emotion"] = rate
        data_wrapper.save(tweet)


def calc_words():
    tweets = data_wrapper.get_all_tweets()
    users = {}
    for tweet in tweets:
        if tweet["username"] not in users.keys():
            users[tweet["username"]] = 1
        else:
            users[tweet["username"]] = users[tweet["username"]] + 1
    users_rates = data_wrapper.get_users_rates(users)
    clust1 = []
    clust2 = []
    clust3 = []
    clust4 = []
    for k in users.keys():
        count = users[k]
        rate = users_rates[k]
        if int(count) <= 3 and float(rate) >= float(0):
            clust1.append(k)
        elif int(count) > 3 and float(rate) >= float(0):
            clust2.append(k)
        elif int(count) <= 3 and float(rate) < float(0):
            clust3.append(k)
        else:
            clust4.append(k)
    data = json.dumps({"1": calc_word_for_users(tweets, clust1), "2": calc_word_for_users(tweets, clust2),
                      "3": calc_word_for_users(tweets, clust3), "4": calc_word_for_users(tweets, clust4)})
    return data


def calc_word_for_users(tweets, usernames):
    words = {}
    for tweet in tweets:
        if tweet["username"] in usernames:
            tweet_words = string_split(tweet["tweet_text"])
            for word in tweet_words:
                if word in words.keys():
                    words[word] = words[word] + 1
                else:
                    words[word] = 1
    max_count = 0
    word = ''
    for w in words.keys():
        if words[w] >= max_count:
            max_count = words[w]
            word = w
    return word


# def main():
#     data = prepare_data()
#     test_data = prepare_content()
#     dictionary.init()
#     for l in test_data:
#         # looking for same words only
#         rate, missing_words = rate_string(data, l, 0, True)
#         print("distance:", 0, " string:", l, " rate:", rate, " missing words:", missing_words)
#         rate, missing_words = rate_string(data, l, 1, True)
#         print("distance:", 1, " string:", l, " rate:", rate, " missing words:", missing_words)
#
#     # without oxford dictionary
#     for l in test_data:
#         # looking for same words only
#         rate, missing_words = rate_string(data, l, 0, False)
#         print("distance:", 0, " string:", l, " rate:", rate, " missing words:", missing_words)
#         rate, missing_words = rate_string(data, l, 1, False)
#         print("distance:", 1, " string:", l, " rate:", rate, " missing words:", missing_words)
#     dictionary.store_data_to_file()


def parse_str_to_bool(string):
    return string is not None and string.lower() in ("yes", "true", 1)


@app.route("/rabbit")
def start_rabbit():
    rabbit_thread = threading.Thread(target=rabbit_consumer.init(), daemon=True)
    rabbit_thread.start()
    return "rabbit started"


@app.route("/rate/")
def rate_web_request():
    string = request.args.get("string")
    od = request.args.get("od")
    rate, missing_words = rate_string(data_wrapper.get_data(), string, 0, parse_str_to_bool(od))
    return "rate is:" + str(rate) + ". missing words are:" + str(missing_words)


@app.route("/self_made_data")
def get_self_made_data():
    return data_wrapper.get_self_made_data()


@app.route("/self_made_data/store", methods=['POST'])
def store_self_made_data():
    data_wrapper.store_self_made_data(request.json)
    return ""


@app.route("/load")
def load_words_to_db():
    data_wrapper.load_words_to_db()
    return ""


if __name__ == "__main__":
    dictionary.init()
    rabbit_consumer.init()
    data_wrapper.get_data()
    app.run(port=1488)
