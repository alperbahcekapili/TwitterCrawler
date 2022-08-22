import base64
import streamlit as st
from streamlit_tags import st_tags

import pandas as pd
import tweepy
import datetime
from datetime import timedelta
from datetime import datetime
import time
import requests
import os
import json

from pages.Preprocess import preprocess_downloaded








#curl --get 'https://api.twitter.com/1.1/search/tweets.json' --data '&q=%23archaeology' -H "Authorization: Bearer AAAAAAAAAAAAAAAAAAAAAOHnPAEAAAAAPZ%2BKn6G2Zgb8Cfl56hsrJGkJx2M%3DLUW0gjczHHNdlbz4KWcv22ZWxmmI0491QG87R0MKaJM3ayOz1q"



# 180 requests per 15-minute window per each authenticated user

# so 1 req per 5 seconds
# 1 req == 1 topic

# for each topic wait 5 seconds

# per request 10-100 results

            



import os, datetime


base = os.getcwd()


def thread_function(topics):

    while True:

        if not started:
            break



        start_time = time.time()


        file_prefix =  "data"
        # in data folder
        mydir = os.path.join(os.getcwd(), file_prefix , datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))

        file_prefix = os.path.join(file_prefix, mydir)
        if not os.path.exists(file_prefix):
            os.makedirs(file_prefix)
        # create folder for given time

        

        topic_count = len(topics)
    



        for topic in topics:


            crawler_message.text(  f"Searching for:{topic}")
            file_prefix = os.path.join(file_prefix, topic)

            os.makedirs(file_prefix)
            # create folder for each topic under given times folder


            tweet_fields = ["attachments", "author_id", "context_annotations", "conversation_id", "created_at", "entities", "geo", "id", "in_reply_to_user_id", "lang", "public_metrics",   "possibly_sensitive", "referenced_tweets", "reply_settings", "source", "text", "withheld"]
            user_fields = ["created_at", "description", "entities", "id", "location", "name", "pinned_tweet_id", "profile_image_url", "protected", "public_metrics", "url", "username", "verified", "withheld"]
            
            
            resp = client.search_recent_tweets(query=topic, expansions=["author_id"], max_results = max_count,user_auth=True, tweet_fields= tweet_fields, user_fields= user_fields)
            
        

            data = resp[0]
            includes = resp[1]
            errors = resp[2]
            meta = resp[3]

            # user bio, location, user_created_time, takipci vb., tweet in zamani, quote?, resource(telefon mu)
            # retweet mi? kiminkini, quotation mu? kiminki ve hangi tweet
            # kullanici verified mi? 

            dumps = []

            for tweet in data:
                
                
                temp = preprocess_downloaded(tweet)
                user_found = False
                #st.write()
                for i in range(max_count):
                    
                    temp["user"] = {}
                    if includes["users"][i]["id"] == tweet["author_id"]:

                        user_found = True

                        temp["user"]["description"] = includes["users"][i]["description"]
                        temp["user"]["created_at"] = includes["users"][i]["created_at"]
                        temp["user"]["followers_count"] = includes["users"][i]["public_metrics"]["followers_count"]
                        temp["user"]["verified"] = includes["users"][i]["verified"]
                        temp["user"]["location"] = includes["users"][i]["location"]
                        temp["user"]["id"] = includes["users"][i]["id"]
                        temp["user"]["screen_name"] = includes["users"][i]["username"]
                        break
                    
                if not user_found:
                    crawler_message.text( f"!!!Author has not been found in users for author id: {tweet['author_id']}!!!")
                    
                
                dumps.append(temp)

            with open(os.path.join(file_prefix, topic+".twitter_crawler"), "w", encoding='utf8') as json_file:
                out_str = json.dumps(dumps, ensure_ascii=False, default=str)
                out_str = out_str[1:-1]
                out_str = out_str.replace("}, ", "}\n")
                json_file.write(out_str)


            crawler_message.caption( f"file saved to {mydir}/{topic}")


            file_prefix = file_prefix[: file_prefix.rindex("/")]
            # WARNING here may cause problems in windows machines

            
        file_prefix = file_prefix[: file_prefix.rindex("/")]
        exec_time = time.time() - start_time

        crawler_message.caption( f"Sleeping for {5*topic_count-exec_time} seconds")
        time.sleep(int((5*topic_count-exec_time)+1))





st.header("Please fill needed fields...")


isDevelopment = st.text_input("Are you developer?")


consumer_key = st.empty()
consumer_secret = st.empty()
access_token = st.empty()
access_token_secret = st.empty()


if(isDevelopment != "AlperTheDeveloper" and isDevelopment != "") : 
    consumer_key = st.text_input("consumer_key")
    consumer_secret = st.text_input("consumer_secret")
    access_token = st.text_input("access_token")
    access_token_secret = st.text_input("access_token_secret")
else:
    secrets = json.load(open("secrets.json", "r"))
    access_token_secret = secrets["access_token_secret"]
    access_token = secrets["access_token"]
    consumer_key = secrets["consumer_key"]
    consumer_secret = secrets["consumer_secret"]
    del(secrets)


auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

client = tweepy.Client(
    consumer_key=consumer_key, consumer_secret=consumer_secret,
    access_token=access_token, access_token_secret=access_token_secret
)



st.write("You can crawl tweets easily with this tool. You need to provide these to inputs.\n")


topics = st_tags(
    label='# Enter Topics:',
    text='Press enter to add more',
    value=[],
    key='1')




max_count = st.number_input('Max results in one seach(100 max)', 10)

started = False
crawler_status = st.empty()
crawler_message = st.empty()


col1, col2 = st.columns(2)

if col2.button("Stop Crawling"):
    if not started:
        crawler_message.text( "Crawler stopped")
    else:
        crawler_status.text(  "Stopping crawler when it wakes")
        started = False


if col1.button('Start Crawling'):
    if started: 
        crawler_message.text( "Crawler is already active!!!")
    else:
        crawler_status.text( "Crawler is active")
        started = True
        thread_function(topics)


        # user bio, location, user_created_time, takipci vb., tweet in zamani, quote?, resource(telefon mu)
        # retweet mi? kiminkini, quotation mu? kiminki ve hangi tweet
        # kullanici verified mi? 


