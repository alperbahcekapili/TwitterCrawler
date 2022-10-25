from multiprocessing import Process, Queue
import streamlit as st
import json
import random
import os
import redis
import time
import pandas as pd
from redis.commands.json.path import Path
import sys
sys.path.insert(0, os.path.join(os.getcwd(), '/Postprocess_scripts'))

def is_retweet_processed(tweet):
    # input: pd.Dataframe row

    # 3rd index retweet
    # 4th index RT @b33chichi: In honor of chp 402.1 bc this was literally him,

    if tweet.iloc[3] == "retweet" or "RT @" in tweet.iloc[4]:
        return tweet.iloc[4].split("@")[1].split(":")[0]
    
    return None





r = redis.Redis(host='localhost', port=6379, db=0)

st.set_page_config(page_title="User Based Dashboard", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)


st.title("User Based Dashboard")




selection = st.selectbox("Select the loading format", ["","Dump", "Processed"])
if "User_Based_Dashboard:need_refresh" not in st.session_state:
    st.session_state["User_Based_Dashboard:need_refresh"] = True


if selection == "Dump":


    dump_path = st.text_input("Give dump path to generate user dashboards...")
    # here each line contains a tweet object

    user_stats = pd.DataFrame(columns=["username", "location", "Stance", "Age", "Sex"])
    users = set()

    count = 0


    if dump_path:
        # Now we should preprocess files recursively
        current_folder = dump_path
        to_be_processed_file_list = os.listdir(current_folder)
        templist = []
        for i in range(len(to_be_processed_file_list)):
            templist.append(os.path.join(current_folder , to_be_processed_file_list[i]))

        to_be_processed_file_list = templist
        
        index = 0


        start_time = time.time()
            
        
        
        while True:
            if len(to_be_processed_file_list) <= index:
                break

            isDir = os.path.isdir(to_be_processed_file_list[index])
            if isDir: 
                new_file_list = os.listdir(to_be_processed_file_list[index])
                to_be_appended = list(map(lambda x  : os.path.join(to_be_processed_file_list[index], x), new_file_list))
                to_be_processed_file_list.extend(to_be_appended)
                to_be_processed_file_list.remove(to_be_processed_file_list[index])


            else: 
        
        
        
                dump_file = open(to_be_processed_file_list[index], "r")


                while True:
                    count += 1
                
                    # Get next line from file
                    line = dump_file.readline()
                
                    
                    # if line is empty
                    # end of file is reached
                    if not line:
                        break
                    

                    tweet = json.loads(line)

                    username = tweet["user"]["screen_name"]
                    userid = tweet["user"]["id"]
                    if 0 == r.exists(userid):
                        r.set(str(userid), tweet["text"])
                    else:
                        r.set(str(userid), "|||".join((r.get(userid).decode("utf-8"),tweet["text"])))

                    if userid not in users:
                        users.add(userid)
                        #user_stats[userid] = {"id": userid, "username":username}
                        user_stats.loc[userid] = [username, None, None, None, None, None]
                    

                index+=1
        dump_path=False



        st.write("--- total parsing in %s seconds ---" % (time.time() - start_time))
        st.write(r.dbsize())
        st.write("To inspect a user, select from the following list")




        user_stats_container = st.empty()



elif selection == "Processed":
    processed_path = st.text_input("Give processed tweet root directory path to generate user dashboards...")
    if processed_path:
        # Now we should preprocess files recursively
        current_folder = processed_path
        to_be_processed_file_list = os.listdir(current_folder)
        templist = []
        for i in range(len(to_be_processed_file_list)):
            templist.append(os.path.join(current_folder , to_be_processed_file_list[i]))

        to_be_processed_file_list = templist
        
        index = 0


        start_time = time.time()
            
        user_stats = pd.DataFrame(columns=["Userid", "Username", "Userloc", "Stance", "Age"])
        users = []

        while True:
            if len(to_be_processed_file_list) <= index:
                break

            isDir = os.path.isdir(to_be_processed_file_list[index])
            if isDir: 
                new_file_list = os.listdir(to_be_processed_file_list[index])
                to_be_appended = list(map(lambda x  : os.path.join(to_be_processed_file_list[index], x), new_file_list))
                to_be_processed_file_list.extend(to_be_appended)
                to_be_processed_file_list.remove(to_be_processed_file_list[index])


            else: 
        
                dump_file = pd.read_csv(to_be_processed_file_list[index] , lineterminator = "\n")
                count = 0
                for index, row in dump_file.iterrows():
                    count += 1
                    username = row["user_screen_name"]
                    userid = row["user_id"]
                    userloc = row["user_location"]

                    if userid not in users:
                        users.append(userid)
                        user_stats.loc[len(user_stats.index)] = [userid, username, userloc, "Good", 21]


                    



                index+=1
        st.write("--- total parsing in %s seconds ---" % (time.time() - start_time))
        print(user_stats)
        st.dataframe(user_stats, width = 1500, height = 600)

