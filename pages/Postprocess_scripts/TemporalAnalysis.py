import json
import pandas as pd
from datetime import datetime



def get_date(tweet):
    tweet_time = tweet["data"]["created_at"]
    time_obj_str = ":".join(tweet_time.split(":")[:-1])
    #time_obj = datetime.fromisoformat(tweet_time)
    #time_obj_str = time_obj.strftime("%m.%d.%Y")
    #print("Extracted date: " + time_obj_str, "Type: ", type(time_obj_str))
    return time_obj_str
def add_to_entry(index, dict_obj ,counts):
    # index: date of the tweet
    for label in list(counts.keys()):
        dict_obj[index][label] += counts[label]
    return dict_obj

def get_labels_obj(labels_file):
    labels_pr = json.load(open(labels_file, "r"))        
    labels  = {}
    for el in labels_pr:
        labels[list(el.keys())[0]] = el[list(el.keys())[0] ]
    return labels