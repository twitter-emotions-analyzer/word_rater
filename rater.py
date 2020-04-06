import json
import dictionary


# reads words dictionary into python dictionary
def prepare_data():
    with open("en_words.json") as file:
        data = json.load(file)
    return data


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


def rate_string(data, string_to_check, d=1, use_od=False):
    rate = 0
    rate_before = 0
    out = []
    # iterating over all words in provided string
    words = string_split(string_to_check)
    for l in words:
        found_words = {}  # dict of words that might be same as one in string
        for p in data.keys():
            if are_strings_equal(l, p, d):
                found_words.update({p: data[p]})
        if len(found_words) > 0:
            found = False
            for w in found_words.keys():
                if are_strings_equal(w, l, 0):  # first we need to check if the same word has been found
                    rate += found_words[w]
                    found = True
                    break
            if not found:  # otherwise let's normalize all found odds
                tmp_rate = 0
                for w in found_words.keys():
                    tmp_rate += found_words[w]
                tmp_rate /= len(found_words)
                rate += tmp_rate
        if rate == rate_before:  # if nothing has been found this word should be highlighted
            # first - let's check oxford dictionary
            if use_od:  # if it's original string, not the one from OD
                od_rate = rate_via_od(data, l)
                rate += od_rate
                dictionary.store_data(l, rate)
            else:
                out.append(l)
        else:
            rate_before = rate
    # normalizing string's rate
    if len(words) > 0:
        rate /= max((len(words) - len(out)), 1)
    return rate, out


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


def main():
    data = prepare_data()
    test_data = prepare_content()
    dictionary.init()
    for l in test_data:
        # looking for same words only
        rate, missing_words = rate_string(data, l, 0, True)
        print("distance:", 0, " string:", l, " rate:", rate, " missing words:", missing_words)
        rate, missing_words = rate_string(data, l, 1, True)
        print("distance:", 1, " string:", l, " rate:", rate, " missing words:", missing_words)

    # without oxford dictionary
    for l in test_data:
        # looking for same words only
        rate, missing_words = rate_string(data, l, 0, False)
        print("distance:", 0, " string:", l, " rate:", rate, " missing words:", missing_words)
        rate, missing_words = rate_string(data, l, 1, False)
        print("distance:", 1, " string:", l, " rate:", rate, " missing words:", missing_words)
    dictionary.store_data_to_file()


main()
