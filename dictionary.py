import json
import requests
import argparse

app_id = ""
app_key = ""
language = "en-gb"
url = "https://od-api.oxforddictionaries.com:443/api/v2/entries/" + language + "/"
new_rates = {}


def init():
    global new_rates
    with open("made_dictionary.json") as file:
        new_rates = json.load(file)
    global app_key
    global app_id
    parser = argparse.ArgumentParser("")
    parser.add_argument("app_key", type=str)
    parser.add_argument("app_id", type=str)
    args = parser.parse_args()
    app_id = args.app_id
    app_key = args.app_key


def get_data_from_od(word):
    # get from local dict, if already exist, otherwise use Oxford Dictionary API
    global new_rates
    if word in new_rates:
        return new_rates[word], None
    else:
        response = requests.get(url + word.lower(), headers={"app_id": app_id, "app_key": app_key})
        return None, response.json()


def store_data(word, rate):
    global new_rates
    if not new_rates:
        new_rates = {}
    if word not in new_rates:
        new_rates.update({word: rate})


def store_data_to_file():
    with open("made_dictionary.json", "w") as output:
        json.dump(new_rates, output)
