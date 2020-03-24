import json


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


def rate_string(data, string_to_check, d=1):
    rate = 0
    rate_before = 0
    out = []
    # iterating over all words in provided string
    for l in string_split(string_to_check):
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
            out.append(l)
        else:
            rate_before = rate
    return rate, out


def main():
    data = prepare_data()
    test_data = prepare_content()
    for l in test_data:  # looking for same words only
        rate, missing_words = rate_string(data, l, 0)
        print("distance:", 0, " string:", l, " rate:", rate, " missing words:", missing_words)
    for l in test_data:  # checking with correction
        rate, missing_words = rate_string(data, l)
        print("distance:", 1, " string:", l, " rate:", rate, " missing words:", missing_words)


main()
