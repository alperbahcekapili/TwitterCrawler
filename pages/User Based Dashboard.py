from asyncio import sleep
from multiprocessing import Process, Queue
from pages.Postprocess_scripts.Functions import generate_figure, get_age_interval, get_redis_client, predict_gender, get_recursive_file_list, read_stances
import streamlit as st
import json
import random
import os
import redis
import time
import pandas as pd
from redis.commands.json.path import Path
import sys
from pages.Postprocess_scripts.Stance_Detection import StanceDetection

from pages.Preprocess_scripts.Functions import generate_abrs_object, process_tweet, try_new_locations, visualize_results
sys.path.insert(1, os.path.join(os.getcwd(), 'pages/Postprocess_scripts'))

from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode





r = get_redis_client()
st.title("User Based Dashboard")
selection = st.selectbox("Select the loading format", ["","Dump", "Processed"])
# get names dictionary for rule based gender prediction
names = None
if selection:
    # if now previously calculated
    if "user_stats" not in st.session_state:
        names = pd.read_csv("./local/isimler.csv")
        user_stats = pd.DataFrame(columns=["Username", "Text", "Userloc", "Stance", "Age", "Gender"])
        genders = {
            "male" : 0,
            "female" : 0,
            "unknown" : 0
        }
        stances = {"AKP": 0, "CHP": 0, "IYI PARTI": 0}
        ages = {}
        for i in range(0 ,100, 10):
            ages[f"{i}-{i+10}"] = 0


root = st.text_input("Give processed tweet root directory path to generate user dashboards...")

buttons_pane = st.empty()
statistics_pane = st.empty()
predicted_stats = None




if "user_stats" not in st.session_state:
        
    

    if selection == "Dump" and root:

        
        users = set()
        count = 0

        # Now we should preprocess files recursively
        to_be_processed_file_list= get_recursive_file_list(root)
        index = 0
        start_time = time.time()
            
        
        
        
        for index in range(len(to_be_processed_file_list)):        
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
                [userid, username, usertext ,userloc, userstance, userage, predicted_gender] = process_tweet(tweet ,option=selection, names= names, r = r)
                genders[predicted_gender]+=1
                stances[userstance] += 1
                ages[get_age_interval(interval = 10 ,age = userage)] += 1
                
                if userid not in users:
                    users.add(userid)
                    user_stats.loc[len(user_stats.index)] = [username, usertext ,userloc, userstance, userage, predicted_gender]                
            predicted_stats = (stances, ages, genders)
            visualize_results(predicted_stats=(stances, ages, genders), statistics_pane=statistics_pane, user_stats=user_stats)



        st.write("--- total parsing in %s seconds ---" % (time.time() - start_time))


    elif selection == "Processed" and root:
        
        start_time = time.time()
            
        users = []

        to_be_processed_file_list = get_recursive_file_list(root)
        
        for file in to_be_processed_file_list:
            dump_file = pd.read_csv( file, lineterminator = "\n")
            count = 0
            for index, row in dump_file.iterrows():
                count += 1

                [userid, username, usertext ,userloc, userstance, userage, predicted_gender] = process_tweet(row ,option=selection, names= names, r = r)

                genders[predicted_gender]+=1
                stances[userstance] += 1
                ages[get_age_interval(interval = 10 ,age = userage)] += 1

                if userid not in users:
                    users.append(userid)
                    user_stats.loc[len(user_stats.index)] = [username, usertext ,userloc, userstance, userage, predicted_gender]

            predicted_stats = (stances, ages, genders)  
            visualize_results(predicted_stats=(stances, ages, genders), statistics_pane=statistics_pane, user_stats=user_stats)

        st.write("--- total parsing in %s seconds ---" % (time.time() - start_time))

    if selection and root:
        st.session_state["predicted_stats"] = (stances, ages, genders)
        st.session_state["user_stats"] = user_stats
        visualize_results(predicted_stats=(stances, ages, genders), statistics_pane=statistics_pane, user_stats=user_stats)



    
else:
    visualize_results(predicted_stats=st.session_state["predicted_stats"], statistics_pane=statistics_pane, user_stats=st.session_state["user_stats"])
    








# # stats part
if selection and root:
    
    with buttons_pane.container():

        col1, col2, col3  = st.columns(3, gap = "medium")
        with col1:
            loc_button = st.button("Location Detection")
            
            if loc_button or "Location_Detection_Button_Triggered" in st.session_state:
                st.session_state["Location_Detection_Button_Triggered"] = True
                st.text("Location Detection Button Triggered")
                user_stats = st.session_state["user_stats"]
                (stances, ages, genders) = st.session_state["predicted_stats"]
                abrs_path = st.text_input("Please give abrs file path...")
                     
                if abrs_path:
                    st.text("Abrs path is given...")
                    st.session_state.pop("Location_Detection_Button_Triggered")
                    abrs = generate_abrs_object(abrs_path)
                    detected_users = try_new_locations(st.session_state["user_stats"], abrs)
                    loc_detection_results = ["-" for i in range(len(st.session_state["user_stats"]))]
                    for i in detected_users:
                        loc_detection_results[ i] = "+"
                    st.session_state["user_stats"]["Location Detected"] = loc_detection_results
                    user_stats = st.session_state["user_stats"]
                    visualize_results(predicted_stats=(stances, ages, genders), statistics_pane=statistics_pane, user_stats=user_stats)
                    st.text(f"Totally {len(detected_users)} locations are detected out of {len(user_stats)}, detection rate: {len(detected_users)/ len(user_stats)}")
                        
        with col2:
            stance_button = st.button("Stance Detection")
            if stance_button or "Stance_Detection_Button_Triggered" in st.session_state: 
                st.text("Stance_Detection_Button_Triggered")
                st.session_state["Stance_Detection_Button_Triggered"] = True

                file_path = st.text_input("Stance labels file")


                if file_path:

                    stances = read_stances(file_path)
                    expander = st.expander("Stances")
                    expander.write(stances)


                    preexisting_retweet_user_dict = st.selectbox("I have preexisting user retweets dictionary", ["Yes", "No"])
                    
                    if preexisting_retweet_user_dict == "No":
                        tweets_file_path =  st.text_input("Please provide the root folder that contains processed csv's")
                    elif preexisting_retweet_user_dict == "Yes":
                        tweets_file_path =  st.text_input("Please provide the dictionary file")


                    
                    if tweets_file_path:
                        st.info("Starting Process...")
                        st.session_state["stats_queue"] = Queue(1000)
                        st.session_state["preprocess_process"] =  Process(target=StanceDetection, args=(tweets_file_path, stances, st.session_state["stats_queue"], preexisting_retweet_user_dict == "Yes" ))
                        st.session_state["preprocess_process"].start()


                        stats = st.empty()
                        st.session_state.pop("Stance_Detection_Button_Triggered")

                        while(True):
                            sleep(0.001)
                            if not st.session_state["stats_queue"].empty():
                                response = st.session_state["stats_queue"].get(block=True, timeout=1)
                                if "break" in response.keys():
                                    break
                                
                                if  "user_stance_dict" in response.keys():

                                    user_stance_dict = response["user_stance_dict"]
                                    for usr in user_stance_dict.keys():
                                        # update this user's stance
                                        st.session_state["user_stats"].loc[st.session_state["user_stats"]["Username"] == usr, "Stance"] = user_stance_dict[usr]
                                        

                                
                                    visualize_results(predicted_stats=st.session_state["predicted_stats"], statistics_pane=statistics_pane, user_stats=st.session_state["user_stats"])

                                # with stats.container():
                                #     st.write(response)









        with col3:
            loc_based_det = st.button("Location Based Detection")
            if loc_based_det: 
                st.text("Location Based Detection Button Triggered")
            
        

