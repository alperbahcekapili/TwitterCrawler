import streamlit as st
import pandas as pd

import time  # to simulate a real time data, time loop
import json
import numpy as np  # np mean, np random
import plotly.express as px  # interactive charts
from streamlit_tags import st_tags

import io,json
import os,sys
import gzip
from subprocess import call
import re
import codecs




st.header("Preprocess")
st.write("You can preprocess the tweet you downloaded. You need to upload these tweets as bulk files. Here you will recieve visualizations of the statistics about tweets.")






def getText(data):       
    # Try for extended text of original tweet, if RT'd (streamer)
    try: 
        text = data['retweeted_status']['extended_tweet']['full_text']
    except: 
    # Try for extended text of an original tweet, if RT'd (REST API)
        try: 
            text = data['retweeted_status']['full_text']
        except:
            # Try for extended text of an original tweet (streamer)
            try: 
                text = data['extended_tweet']['full_text']
            except:
                # Try for extended text of an original tweet (REST API)
                try: 
                    text = data['full_text']
                except:
                    # Try for basic text of original tweet if RT'd 
                    try: 
                        text = data['retweeted_status']['text']
                    except:
                        # Try for basic text of an original tweet
                        try: 
                            text = data['text']
                        except: 
                            # Nothing left to check for
                            text = '-'
    return text

def getEntities(data):
    try:
        entities =  data['retweeted_status']['extended_tweet']['entities']
    except: 
        try: 
            entities = data['retweeted_status']['entities'] 
        except:
            try: 
                entities = data['extended_tweet']['entities'] 
            except:
                entities = data['entities'] 
            
    return entities
    
def getMentionedUsers(entities):
    mentionsText = list()
    try:
        mentions =  entities['user_mentions']
	
    except: 
        return None
    
    for m in mentions:
        mentionsText.append( m['screen_name'])
    
    if len(mentionsText) == 0:
       mentionsText.append("null") 
    
    return mentionsText

def getRetweetedUser(data):
    try: 
        text = data['retweeted_status']['user']['screen_name']
    except:
         text = "-"
	
    return text



def getCountsofQuotedTweet(data):
    try: 
        rcount = data['quoted_status']['retweet_count']
        fcount = data['quoted_status']['favorite_count']
        replycount = data['quoted_status']['reply_count']
        qcount = data['quoted_status']['quote_count']
        
    except:
         rcount = 0
         fcount = 0
         replycount = 0
         qcount = 0
	
    return (rcount,fcount,replycount,qcount)

def getCountsofRetweetedTweet(data):
    try: 
        rcount = data['retweeted_status']['retweet_count']
        fcount = data['retweeted_status']['favorite_count']
        replycount = data['retweeted_status']['reply_count']
        qcount = data['retweeted_status']['quote_count']
        
    except:
         rcount = 0
         fcount = 0
         replycount = 0
         qcount = 0
	
    return (rcount,fcount,replycount,qcount)



    
def getQuotedText(data):
    try: 
        text = data['quoted_status']['text'].replace("\n", ' ').replace("\r", " ").replace("\t", " ").replace("  "," ").strip()
    except:
         text = "-"

    return text
    
def getQuotedUser(data):
    try: 
        text = data['quoted_status']['user']['screen_name']
    except:
         text = "-"

    return text

def isabout(keywords, text):
    for k in keywords:
        if k in text:
            return True
        l = text.lower()
        if k in l:
            return True
    return False

def what_isit_about(json_obj, text):
    for topic in json_obj["topics"]:
        if isabout(keywords=topic["keywords"], text = text):
            return topic["name"]
    return -1

def preprocess_text(text):
    text = text.replace("\n", ' ').replace("\r", " ").replace("\t", " ").replace("  "," ").strip()
    return text


def preprocess_tweet(tweet):

    tweet_dict = {}
    
    tweet_dict["time"] = tweet["created_at"]
    tweet_dict["ref"] = tweet["referenced_tweets"]
    tweet_dict["text"] = preprocess_text(tweet["text"])
    tweet_dict["id"] = tweet["id"]

    return tweet_dict

def preprocess_general_dump(json_obj, dump_file_path):

    count = 0
    tc  = 0
    enc = 0
    ntc = 0
    hata = 0
    
    output_folder = "tweets_tsv_format"


    other_texts = {}
    main_texts = {}
    output_folders = {}

    other_languages = ["en", "tr", "de", "cs", "no", "is"]
    for lang in other_languages:
        other_texts[lang] = ""
        output_folders[lang] = os.path.join(output_folder, lang)

    

    for topic in json_obj["topics"]:
        topic_name =  topic["name"]
        main_texts[topic_name] = ""
        output_folders[topic_name] = os.path.join(output_folder, topic["name"])


    

    hata_str = ""
    error_file = output_folder + "/hata"


    for line in gzip.open(dump_file_path):
        try:
            o = json.loads(line)
            text = getText(o).replace("\n", ' ').replace("\r", " ").replace("\t", " ").replace("  "," ").strip()
            date = re.sub("\:.*\+0000", "", o['created_at'])
            date = re.sub("^\S+ ", "", date)
            
            user_created_time = re.sub("\:.*\+0000", "", o['user']['created_at']) 
            user_created_time = re.sub("^\S+ ", "", user_created_time)
            source = re.sub("</a>", "", o['source'])
            source = re.sub("<a.*>", "", source)
            source = re.sub("Twitter for ", "", source)
            
            retweeted_user = getRetweetedUser(o)
            qouted_user =getQuotedUser(o)
            qouted_text =getQuotedText(o)
            
            qrcount,qfcount,qrelycount,qqcount = getCountsofQuotedTweet(o)
           
            rrcount,rfcount,rrelycount,rqcount = getCountsofRetweetedTweet(o)
            
            
            entities = getEntities(o)
            mentionsText= getMentionedUsers(entities)
            in_reply_to = o['in_reply_to_screen_name']

            if str(in_reply_to) == "None":
                in_reply_to = "-"
                in_reply_to_status_id = "-"
            else:
                in_reply_to_status_id = o['in_reply_to_status_id']

            if  o['is_quote_status'] == False:
                quoted_status_id = "-"
            else:
                try: 
                    quoted_status_id = o['quoted_status_id_str']
                except:
                    quoted_status_id = "null"

            urls = list()
            hashtags = list()
            medias = list()
            
            for url in entities['urls']:
                urls.append(url['expanded_url'])
            else:
                if not urls:
                    urls.append("null")

            for hashtag in entities['hashtags']:
                hashtags.append(hashtag['text'])
            else:
                if not hashtags:
                    hashtags.append("null")

            if 'media' in entities:
                for media in entities['media']:
                    medias.append(media['expanded_url'])
            if not medias:
                medias.append("null")   
            
            if not o['user']['description']:
                description = "-"
            else:
                description = o['user']['description'].replace("\n", ' ').replace("\r", " ").replace("\t", " ").replace("  "," ").strip()
                while "  " in description:
                   description = description.replace("  "," ") 
            
            if str(o['place']) != "None":
                tweet_location = o['place']['name'].encode('utf-8')
                tweet_country_code = o['place']['country_code']
            else:
                tweet_location = "-"
                tweet_country_code = "-"

            if str(o['coordinates']) != "None":
                longitude = str(o['coordinates']['coordinates'][0])
                latitude = str(o['coordinates']['coordinates'][1])
            else:
                longitude = "-"
                latitude = "-"
            
            if not o['user']['location']:
                location = "-"
            else:
                location = o['user']['location'].replace('\n', ' ').replace('\r', '').replace('\t', ' ').rstrip('\n').strip()            
            
            a =                o['lang'] +  "\t" + date   + "\t" +  o['id_str']  + "\t" + source
            a = a  + "\t"+     tweet_location.lower()  + "\t" +  tweet_country_code + "\t" +  longitude + "\t" + latitude
            a = a + "\t" +     text  
            a = a + "\t" +      ",".join(mentionsText) + "\t" + ",".join(urls) + "\t" +  ",".join(hashtags) + "\t" +  ",".join(medias) 
            a = a + "\t" + o['user']['screen_name'] + "\t" + o['user']['name'].replace("\n", " ").replace("\r", " ")
            a = a + "\t" + description  + "\t" +   location.lower()  
            a = a + "\t" +  str(o['user']['verified']) + "\t" + str(o['user']['followers_count']) 
            a = a + "\t" +  str(o['user']['friends_count']) + "\t" + str(o['user']['listed_count']) 
            a = a + "\t" +  str(o['user']['favourites_count']) + "\t" + str(o['user']['statuses_count']) 
            a = a + "\t" +  user_created_time + "\t" + str(o['user']['default_profile']) 
            a = a + "\t" +  str(o['user']['default_profile_image']) 
            a = a + "\t" +  retweeted_user+ "\t" + str(in_reply_to_status_id)  + "\t" +  in_reply_to
            a = a + "\t" +  str(quoted_status_id) + "\t" + qouted_text + "\t" + qouted_user 
            a = a + "\t" +  str(o['retweet_count']) +  "\t" + str(o['favorite_count'])
            a = a + "\t" +  str(o['reply_count']) +  "\t" + str(o['quote_count'])
            a = a + "\t" +  str(qrcount) +  "\t" +  str(qfcount) +  "\t" +  str(qrelycount) +  "\t" +  str(qqcount)
            a = a + "\t" +  str(rrcount) +  "\t" +  str(rfcount) +  "\t" +  str(rrelycount) +  "\t" +  str(rqcount) + "\n"
            



            if o['lang'] == "en":
                topic = what_isit_about(json_obj, text)
                if topic == -1:
                    other_texts["en"] = other_texts["en"] + a
                    output_folders["en"] = os.path.join(output_folder, "en", "other")

                else:
                    main_texts[topic] = main_texts[topic] + a
                    output_folders[topic] = os.path.join(output_folder, "en", topic)

                enc = enc + 1

            elif o['lang'] == "tr":
                topic = what_isit_about(json_obj, text)

                if topic == -1:
                    other_texts["tr"] = other_texts["tr"] + a
                    output_folders["tr"] = os.path.join(output_folder, "tr", "other")
                else:
                    main_texts[topic] = main_texts[topic] + a
                    output_folders[topic] = os.path.join(output_folder, "tr", topic)
                
                tc = tc + 1
            
            elif o['lang'] == "de":           
                other_texts["de"] = other_texts["de"] + a
                output_folders["de"] = os.path.join(output_folder, "de")

            elif o['lang'] == "cs":
                other_texts["cs"] = other_texts["cs"] + a
                output_folders["cs"] = os.path.join(output_folder, "cs")
            elif o['lang'] == "no":
                other_texts["no"] = other_texts["no"] + a
                output_folders["no"] = os.path.join(output_folder, "no")
            elif o['lang'] == "is":
                other_texts["is"] = other_texts["is"] + a
                output_folders["is"] = os.path.join(output_folder, "is")
                
            else:
                 outputfile = output_folder + "/" + o['lang']
                 with io.open(outputfile, 'a',encoding='utf-8') as f:
                         f.write(a)
                 ntc = ntc + 1



        except Exception as e:
            print(e)
            hata = hata + 1
            hata_str = hata_str + str(line) + "\n"
            
            pass
            count = count + 1
    print(str(count) + "\t" + str(tc) + "\t" + str(enc) + "\t" + str(hata))
        
    
    from datetime import datetime
    now = str(datetime.now())




    for topic in main_texts:
        new_file_path = os.path.join(now, output_folders[topic])
        new_file_folder = new_file_path[0:new_file_path.rindex("/")]
        if not os.path.exists(new_file_folder):
            os.makedirs(new_file_folder)
        st.write(new_file_path)
        with io.open(new_file_path, 'w+',encoding='utf-8') as f:
            f.write(main_texts[topic])
        
    for topic in other_texts:
        new_file_path = os.path.join(now, output_folders[topic])
        new_file_folder = new_file_path[0:new_file_path.rindex("/")]
        if not os.path.exists(new_file_folder):
            os.makedirs(new_file_folder)

        with io.open(os.path.join(now, output_folders[topic]), 'w+',encoding='utf-8') as f:
            f.write(other_texts[topic])
        
    with io.open(os.path.join(now, error_file), 'w+',encoding='utf-8') as f:
        f.write(hata_str)

json_file_path = st.text_input("Please give your topics and keywords as a json file")
if json_file_path:
    st.write(f"Json File Path: {json_file_path}")
    json_file = open(json_file_path, "r")
    json_obj = json.load(json_file)
    dump_file_path = st.text_input("Please give the path of dump")
    if dump_file_path:
        preprocess_status = st.empty()
        preprocess_status.text( "Preprocessing...")
        preprocess_general_dump(json_obj, dump_file_path)
        preprocess_status.text( "Preprocess ended...")
