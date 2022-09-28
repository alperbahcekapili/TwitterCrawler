from datetime import datetime
from tracemalloc import start
from types import resolve_bases
import streamlit as st
import pandas as pd
import json
import numpy as np  # np mean, np random
from streamlit_tags import st_tags
import io,json
import os,sys
import gzip
import subprocess
import time
from multiprocessing import Process, Queue
from multiprocessing import SimpleQueue


st.set_page_config(page_title="Preprocess", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)

# adding Folder_2 to the system path
sys.path.insert(0, os.path.join(os.getcwd(), '/Preprocess_scripts'))

from pages.Preprocess_scripts.PreprocessDump import PreprocessDump



stats = st.empty()



def is_dict_empty(tweets):
    
    for key1 in tweets:
        for key2 in tweets[key1]:
            try:
                if not tweets[key1][key2].empty:
                    return False
            except:
                # this error means that entry has been set to None
                # no problem throwing exception here
                pass
    return True

def save_dictionary(tweets, output_folder):
    for key1 in tweets:
        for key2 in tweets[key1]:
            out_file = os.path.join(output_folder)
            if not os.path.exists(out_file):
                os.makedirs(out_file)

            out_file = os.path.join(output_folder, key1)
            if not os.path.exists(out_file):
                os.makedirs(out_file)

            out_file = os.path.join(output_folder, key1, key2)
            if not os.path.exists(out_file):
                os.makedirs(out_file)


            current_time = datetime.now()
            out_file = os.path.join(output_folder, key1, key2, str(current_time))
            
            print(out_file)
            try:
                tweets[key1][key2].to_csv(out_file+ ".csv", index = False)
            except:
                # this means this entry has no been initialize in this batch
                pass

    clear_dictionary(tweets)


def clear_dictionary(tweets):
    for key1 in tweets:
        for key2 in tweets[key1]:
            tweets[key1][key2] = None

def max_mem_reached(dicto, max_size):

    total_size = 0
    for key1 in dicto:
        for key2 in dicto[key1]:
            total_size += sys.getsizeof(dicto[key1][key2])
    # mb to bytes
    if total_size > max_size * 1000000:
        return True
    
    return False
    
    

def isabout(keywords, text):
    for k in keywords:
        if k in text:
            return True
        l = text.lower()
        if k in l:
            return True
    return False


def what_isit_about(json_obj, text):
    relevant_topics = []
    index = 0
    for topic in json_obj:
        if isabout(keywords=topic["keywords"], text = text):
            relevant_topics.append(index)
        index+=1
    return relevant_topics


def update_stats(topic, lang):
    if topic in st.session_state["tweets"]:
        st.write(st.metric("Total tweets", len(st.session_state["tweets"][topic][lang].index )) 
)

def update_revealed_langs(topic, revealed_langs):
    if topic != "":
        revealed_langs = st.session_state["tweets"][topic].keys()
    



def preprocess_text(text):
    text = text.replace("\n", ' ').replace("\r", " ").replace("\t", " ").replace("  "," ").strip()
    return text

def remove_new_lines(input):
    return str(input).replace("\n", " ")

def preprocess_downloaded(tweet):

    tweet_dict = {}
    tweet_dict["created_at"] = remove_new_lines(tweet["created_at"])
    tweet_dict["in_reply_to_user_id"] = remove_new_lines(tweet["in_reply_to_user_id"])

    try:
        #twitter api v2 retweet or quote distinction
        #this will throw error 
        tweet_dict["referenced_tweet"] = remove_new_lines(tweet["referenced_tweets"].split("id=")[-1].split[" "][0])
        #extract id from ref entry

        if "replied_to" in str(tweet["referenced_tweets"][-1]):
            tweet_dict["ref_type"] = "quote"
        if "retweeted" in str(tweet["referenced_tweets"][-1]):
            tweet_dict["ref_type"] = "retweet"

    except:
        # twitter api v1 retweeted_status field is used to distinguish a retweet
        try:
            
            # this will throw exception if retweeted_status does not occur in response
            tweet_dict["referenced_tweet"] = "{0}".format(tweet["retweeted_status"]["id"])
            tweet_dict["ref_type"] = "retweet"

        except: 
            try: 
                # this will throw exception if quoted_status does not occur in response
                tweet_dict["referenced_tweet"] = "{0}".format(tweet["quoted_status"]["id"])
                tweet_dict["ref_type"] = "quote"

            except: 
                # this part is for the format that is downloaded with this tool
                try:
                    tweet_dict["referenced_tweet"] = tweet_dict["referenced_tweet"]
                    tweet_dict["ref_type"] = tweet_dict["ref_type"]
                except:
                    # no matching format so returning None for references
                    tweet_dict["referenced_tweet"] = None
                    tweet_dict["ref_type"] = None




    tweet_dict["text"] = preprocess_text(tweet["text"])
    tweet_dict["id"] = remove_new_lines(tweet["id"])
    tweet_dict["source"] = remove_new_lines(tweet["source"])
    tweet_dict["lang"] = remove_new_lines(tweet["lang"])


    return tweet_dict







































st.header("Preprocess")
st.write("You can preprocess the tweet you downloaded. You need to upload these tweets as bulk files. Here you will recieve visualizations of the statistics about tweets.")



json_file_path = st.text_input("Please give your topics and keywords as a json file")
if json_file_path:
    st.write(f"Json File Path: {json_file_path}")
    try:
        json_file = open(json_file_path, "r")
        json_obj = json.load(json_file)
    except Exception as e:
        st.write(e)
        st.stop()
    dump_file_path = st.text_input("Please give the path of dump")

    if dump_file_path:
        preprocess_status = st.empty()
        max_mem = st.number_input("Maximum memory available",10)

        col1, col2, col3 = st.columns(3)
        with col1:
            start_preprocess = st.button("Start Preprocess")
        with col2:
            stop_preprocess = st.button("Stop Preprocess")
        with col3:
            recieve_stats = st.button("Get Statistics")

        st.session_state["stop_preprocessing"] = False


        if stop_preprocess:
            start_preprocess = False
            st.session_state["stop_preprocessing"] = True
            st.write("Stopping...")
            if "preprocess_process" in st.session_state:
                st.session_state["preprocess_process"].terminate()


        if start_preprocess:
            st.write("Starting...")
            st.session_state["task_queue"] = Queue(1000)
            st.session_state["stats_queue"] = Queue(1000)
            st.session_state["preprocess_process"] =  Process(target=PreprocessDump, args=(json_file_path, dump_file_path, max_mem, st.session_state["task_queue"] , st.session_state["stats_queue"] ))
            st.session_state["preprocess_process"].start()

        stat_view = st.empty()

        if recieve_stats:
            if not stop_preprocess:
                st.session_state["task_queue"].put(("Topic", "Language"), block=True)
                if not st.session_state["stats_queue"].empty():
                    response = st.session_state["stats_queue"].get(block=True, timeout=1)
                    with stat_view.container():
                        st.dataframe(response)
                        time.sleep(1)

