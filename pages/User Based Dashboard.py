import streamlit as st
import json
import random
import os
import redis
import time
import pandas as pd
from redis.commands.json.path import Path

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
        #st.session_state["User_Based_Dashboard:need_refresh"] = False

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
    user_stats = pd.DataFrame(columns=["username", "location", "Stance", "Age", "Sex"]) if "user_stats" not in st.session_state else st.session_state["user_stats"]
    users = set()

    count = 0

    if processed_path:
        #st.session_state["User_Based_Dashboard:need_refresh"] = False
        # Now we should preprocess files recursively
        current_folder = processed_path
        to_be_processed_file_list = os.listdir(current_folder)
        templist = []
        for i in range(len(to_be_processed_file_list)):
            templist.append(os.path.join(current_folder , to_be_processed_file_list[i]))

        to_be_processed_file_list = templist
        
        index = 0


        start_time = time.time()
            
        
        
        while True and "user_stats" not in st.session_state:
            if len(to_be_processed_file_list) <= index:
                break

            isDir = os.path.isdir(to_be_processed_file_list[index])
            if isDir: 
                new_file_list = os.listdir(to_be_processed_file_list[index])
                to_be_appended = list(map(lambda x  : os.path.join(to_be_processed_file_list[index], x), new_file_list))
                to_be_processed_file_list.extend(to_be_appended)
                to_be_processed_file_list.remove(to_be_processed_file_list[index])
            else: 
                processed_tweets = pd.read_csv(to_be_processed_file_list[index], lineterminator='\n')
                for index, row in processed_tweets.iterrows():
                    count += 1
                    username = row.iloc[-1]
                    userid = str(row.iloc[-2])
                    userloc = row.iloc[-3]



                    # create a json object to store

                    # location
                    # retweeted users
                    

                    if 0 == r.exists(str(userid)):
                        retweeted_user = is_retweet_processed(row)
                        to_store = json.dumps({"id": userid, "screen_name": username, "location": userloc, "retweeted_users": [] if retweeted_user is None else [retweeted_user] })
                        r.set(str(userid),to_store)
                        if userid not in users:
                            users.add(userid)
                            #user_stats[userid] = {"id": userid, "username":username, "location": userloc.lower()}
                            user_stats.loc[userid] = [username, userloc.lower(), None, None, None]
                    else:
                        to_deser = r.get(userid)
                        existing_entry = json.loads(to_deser)
                        # check if this tweet is RT and that user exist in the RT list
                        retweeted_user = is_retweet_processed(row)
                        if retweeted_user is not None and retweeted_user not in existing_entry["retweeted_users"]:
                            existing_entry["retweeted_users"].append(retweeted_user)

                        r.set(userid, json.dumps(existing_entry))

                        if userid not in users:
                            users.add(userid)
                            #user_stats[userid] = {"id": userid, "username":username, "location": existing_entry["location"]}
                            user_stats.loc[userid] = [username, existing_entry["location"], None, None, None]
                index+=1

    # lokasyon, stance, yaş, cinsiyet
    cols = st.columns(6) 
    field_labels = ["Id", "Username", "location", "Stance", "Age", "Sex"]
    fields = ["id", "username", "location"]

    random_choices= [["Ankara", "Yalova", "Adana"], 
    ["Positive", "Very Positive","Negative", "Very Negative"],
    ["10", "12", "23", "59", "42", "34", "51"],
    ["Male", "Female"]]

    # for i in range(10):
    #     index = 0
    #     for col in cols:    
    #         with col:
    #             if i == 0:
    #                 st.subheader(field_labels[index])
    #             if index < 3:    
    #                 st.write(user_stats[list(user_stats.keys())[i]][fields[index]])
    #             else:
    #                 st.write(random_choices[index-2][random.randint(0,len(random_choices[index-2])-1)])
    #         index +=1

    if "user_stats" not in st.session_state:
        st.dataframe(user_stats, height = 1000)


    # cansin location detection needed file format is specified in the readme
    detect_locations = st.button("Cansin Location Generate")
    if detect_locations or ("cansin_loc_det" in st.session_state): 
        st.session_state["cansin_loc_det"] = True
        locations_path = st.text_input("Please provide the locations file in the format given in the readme file")
        if locations_path:
            locations_file = open(locations_path, "r")
            locations = {}
            all_abbrs = ""
            for line in locations_file.readlines():
                line_slpitted1 = line.split(":")
                base_location = line_slpitted1[0].lower()
                line_slpitted2 = line_slpitted1[1].split(",")
                locations[base_location] =  list(map(lambda x: x.lower(), line_slpitted2))
                for abr in line_slpitted2:
                    all_abbrs+=","+abr.lower()



            # generated all base locations
            # now we should parse tweets to see if we can find locations of users

            # stats for location detections
            detected_users = 0
            undetected_users = 0
            detection_rate = 0
            # list(user_stats.keys())
            # iterate over all users
            for user in list(user_stats.index.values):
                user_stats.loc[user,"location"] = user_stats.loc[user,"location"].lower()

                did_detect = False
                # iterate over all locaitons
                for base_loc in list(locations.keys()):
                    if user_stats.loc[user,"location"] in locations[base_loc] or user_stats.loc[user,"location"] in base_loc:                        
                        to_deser = r.get(str(user))
                        existing_entry = json.loads(to_deser)
                        existing_entry["location"] = base_loc
                        r.set(str(user), json.dumps(existing_entry))
                        user_stats.loc[user,"location"]=base_loc

                        # update locations in the database
                        detected_users+=1
                        did_detect = True
                        break
                    
                    #print(f"baseloc: {base_loc}, los: {locations[base_loc]}, userloc: {user_stats.loc[user,'location']}")

                    for abr_temp in locations[base_loc]:
                        if user_stats.loc[user,"location"] in abr_temp:
                            user_stats.loc[user,"location"]=base_loc
                            to_deser = r.get(str(user))
                            existing_entry = json.loads(to_deser)
                            existing_entry["location"] = base_loc
                            r.set(str(user), json.dumps(existing_entry))
                            # update locations in the database

                            detected_users+=1
                            did_detect = True
                            break
                                
                    if did_detect:
                        break


                if not did_detect:
                    undetected_users+=1
            if "user_stats" in st.session_state:
                st.dataframe(user_stats, height = 1000)

            to_save_dataframe = user_stats.iloc[:, 0:2]
            to_save_dataframe.to_csv("Updated_Locations.csv")
            detection_rate = detected_users / (detected_users + undetected_users)
            if "user_stats" not in st.session_state:
                st.session_state["user_stats"] = user_stats
            # updated all stats we can print them
            st.write(f"Detection Rate: {detection_rate} Detected Users: {detected_users} Undetected Users: {undetected_users}")





new_dashboard = st.button("New Dashboard")
if new_dashboard:
    st.session_state["User_Based_Dashboard:need_refresh"] = True
