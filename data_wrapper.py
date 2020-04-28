import json

import rater

rate_data = {}
self_made_data = {}


# reads words dictionary into python dictionary
def get_data():
    global rate_data
    if not rate_data:
        with open("en_words.json") as file:
            rate_data = json.load(file)
    return rate_data


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
