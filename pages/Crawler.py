import base64
import streamlit as st
from streamlit_tags import st_tags

import pandas as pd
import tweepy
import datetime
from datetime import timedelta
from datetime import datetime

import asyncio


import time


import threading


import requests
import os
import json

from pages.Preprocess import preprocess_tweet





# [11:01, 21.06.2022] Başak: Ao71uECbsKVhTDAlfCQlDwH9D
# [11:01, 21.06.2022] Başak: Api key
# [11:02, 21.06.2022] Başak: BXkLLCSiTWVdEXnEpRXsTBRGhaPauWGCS6UnOMnTwRMVS8NsWl
# [11:02, 21.06.2022] Başak: Api key secret
# [11:02, 21.06.2022] Başak: 784850369008529409-jvv6oMyYOJErcanHHdAs8hZCVO2BtOb
# [11:02, 21.06.2022] Başak: Access token
# [11:02, 21.06.2022] Başak: OrcSpI3rh7D8P5SlY8wSIvX2BkXqfVhqgBt2ttSzRP6ip
# [11:02, 21.06.2022] Başak: Access token secret


# Your app's API/consumer key and secret can be found under the Consumer Keys
# section of the Keys and Tokens tab of your app, under the
# Twitter Developer Portal Projects & Apps page at
# https://developer.twitter.com/en/portal/projects-and-apps
consumer_key = "Ao71uECbsKVhTDAlfCQlDwH9D"
consumer_secret = "BXkLLCSiTWVdEXnEpRXsTBRGhaPauWGCS6UnOMnTwRMVS8NsWl"

# Your account's (the app owner's account's) access token and secret for your
# app can be found under the Authentication Tokens section of the
# Keys and Tokens tab of your app, under the
# Twitter Developer Portal Projects & Apps page at
# https://developer.twitter.com/en/portal/projects-and-apps
access_token = "784850369008529409-jvv6oMyYOJErcanHHdAs8hZCVO2BtOb"
access_token_secret = "OrcSpI3rh7D8P5SlY8wSIvX2BkXqfVhqgBt2ttSzRP6ip"




#bearer: AAAAAAAAAAAAAAAAAAAAAOHnPAEAAAAAPZ%2BKn6G2Zgb8Cfl56hsrJGkJx2M%3DLUW0gjczHHNdlbz4KWcv22ZWxmmI0491QG87R0MKaJM3ayOz1q

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
            st.w
            break



        start_time = time.time()


        file_prefix =  "data"
        # in data folder
        mydir = os.path.join(os.getcwd(), file_prefix , datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S'))

        file_prefix = os.path.join(file_prefix, mydir)
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

                temp = {}

                user_found = False

                for i in range(max_count):

                    temp["user"] = {}
                    if includes["users"][i]["id"] == tweet["author_id"]:

                        user_found = True

                        temp["user"]["bio"] = includes["users"][i]["description"]
                        temp["user"]["created_time"] = includes["users"][i]["created_at"]
                        #temp["user_public_metrics"] = includes["users"][i]["user_public_metrics"]
                        temp["user"]["verified"] = includes["users"][i]["verified"]
                        temp["user"]["location"] = includes["users"][i]["location"]
                        temp["user"]["id"] = includes["users"][i]["id"]
                        temp["user"]["username"] = includes["users"][i]["username"]
                        break
                    
                if not user_found:
                    crawler_message.text( f"!!!Author has not been found in users for author id: {tweet['author_id']}!!!")
                    

                    

                temp["tweet"] = preprocess_tweet(tweet)
                
                dumps.append(temp)



            with open(os.path.join(file_prefix, topic+".json"), "w", encoding='utf8') as json_file:
                json.dump(dumps, json_file, ensure_ascii=False, default=str)

            crawler_message.caption( f"file saved to {mydir}/{topic}")


            file_prefix = file_prefix[: file_prefix.rindex("/")]
            # WARNING here may cause problems in windows machines

            
        file_prefix = file_prefix[: file_prefix.rindex("/")]
        exec_time = time.time() - start_time

        crawler_message.caption( f"Sleeping for {5*topic_count-exec_time} seconds")
        time.sleep(int((5*topic_count-exec_time)+1))






















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



# start = st.date_input(
#      "Start Date",
#      datetime.date(2019, 7, 6))
# st.write('Start date:', start)


# end = st.date_input(
#      "End Date",
#      datetime.date(2019, 7, 23))
# st.write('End date:', end)




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
    # Search Recent Tweets

    # This endpoint/method returns Tweets from the last seven days

    if started: 
        crawler_message.text( "Crawler is already active!!!")
    else:
        crawler_status.text( "Crawler is active")
        started = True
        thread_function(topics)



    

                    

        
                



    # st.write(str(tweets))
    # file.write(str(tweets))
    # file.close()
    # os.chdir("..")




        # By default, this endpoint/method returns 10 results
        # You can retrieve up to 100 Tweets by specifying max_results



        # user bio, location, user_created_time, takipci vb., tweet in zamani, quote?, resource(telefon mu)
        # retweet mi? kiminkini, quotation mu? kiminki ve hangi tweet
        # kullanici verified mi? 






    



# st.write("Download the crawled tweet. dowload demo")


# @st.cache
# def convert_df():
#     df = pd.read_csv("dir/file.csv")    
#     return df.to_csv().encode('utf-8')


# csv = ""


# st.download_button(
#    "Press to Download",
#    csv,
#    "file.csv",
#    "text/csv",
#    key='download-csv'
# )





filename = "deneme"
b64 = base64.b64encode("alper".encode()).decode()
href = f'<a href="data:file/zip;base64,{b64}" download=\'{filename}.zip\'>\
    Click to download\
</a>'
st.sidebar.markdown(href, unsafe_allow_html=True)