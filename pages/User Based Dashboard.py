from asyncio import sleep
from multiprocessing import Process, Queue

from pages.Postprocess_scripts.Page__Location_Based_Stance_Detection import calculate_support_ratios
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



import pydeck as pdk


r = get_redis_client()
st.title("User Based Dashboard")
selection = st.selectbox("Select the loading format", ["","Dump", "Processed"])
# get names dictionary for rule based gender prediction
names = None
if selection:
    # if now previously calculated
    if "user_stats" not in st.session_state:
        names = pd.read_csv("./local/isimler.csv")
        user_stats = pd.DataFrame(columns=["Userid", "Username", "Text", "Userloc", "Stance", "Age", "Gender"])
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
                    user_stats.loc[len(user_stats.index)] = [userid, username, usertext ,userloc, userstance, userage, predicted_gender]                
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
                    user_stats.loc[len(user_stats.index)] = [userid, username, usertext ,userloc, userstance, userage, predicted_gender]

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
                                    st.session_state["user_stats"]["Stance Detected"] = "-"
                                    for usr in user_stance_dict.keys():
                                        # update this user's stance
                                        st.session_state["user_stats"].loc[st.session_state["user_stats"]["Username"] == usr, "Stance"] = user_stance_dict[usr]
                                        st.session_state["user_stats"].loc[st.session_state["user_stats"]["Username"] == usr, "Stance Detected"] = "+"
                                        

                                
                                    visualize_results(predicted_stats=st.session_state["predicted_stats"], statistics_pane=statistics_pane, user_stats=st.session_state["user_stats"])

                                









        with col3:
            loc_based_det = st.button("Location Based Detection")
            if loc_based_det or "Location_Based_Detection_Triggered" in st.session_state: 
                st.text("Location_Based_Detection_Triggered")
                st.session_state["Location_Based_Detection_Triggered"] = True

                # /home/alper/Documents/Twitter Crawler Documents/GermanyElections/Voters.csv
                electorants =  st.text_input("Please give the total electorants file")
                all_given = electorants 
                if all_given:
                    # read electorant stats and print them

                    electorants = pd.read_csv(electorants, lineterminator="\n")
                    st.dataframe(electorants)
                    electorants_date = electorants.iloc[0, :]

                    support_ratios, support_values = calculate_support_ratios(electorants_date)

                    
                    for loc in support_values.keys():
                        max = -1
                        max_state = ""
                        for stance in support_values[loc].keys():
                            if support_values[loc][stance] > max:
                                max = support_values[loc][stance]
                                max_stance = stance
                        support_values[loc]["stance"] = str(max_stance) + ":" + f"{max}" 
                    
                    # create map with support values
                    
                    st.write(support_ratios)

                    import numpy as np
                    from geopy.geocoders import Nominatim
                    import folium
                    from streamlit_folium import st_folium

                    
                    geolocator = Nominatim(user_agent="streamlit app")
                    # get locs to display on map
                    locs = {}
                    locs_list = []
                    for loc in support_values.keys():
                        locs[loc] = geolocator.geocode(loc, geometry="geojson").raw

                        # get max of 
                        locs[loc]["stance"] = support_values[loc]["stance"]
                        print(locs[loc])
                        for key in locs[loc].keys():
                            try:
                                locs[loc][key] = float(locs[loc][key])
                            except:
                                pass
                        
                        new_obj = dict()
                        new_obj["lat"] = locs[loc]["lat"]
                        new_obj["lon"] = locs[loc]["lon"]
                        new_obj["stance"] = locs[loc]["stance"]
                        new_obj["geometry"] = locs[loc]["geojson"]

                        locs_list.append(new_obj)

                        

                    
                    st.session_state["Location_Supports"] = support_values
                    st.session_state["Support_Ratios"] = support_ratios
                    st.session_state["Loc_Information"] = locs
                    st.session_state["Loc_List"] = locs_list
                
    
    if "Loc_Information" in st.session_state:
        # map = folium.Map( scrollWheelZoom = False, dragging=False)
        # st_map = st_folium(map, width=700, height = 400)
                    
                    

        chart_data = st.session_state["Loc_List"]
        
        mean_lat = 0
        mean_lon = 0
        
        for entry in chart_data:
            mean_lat+=entry["lat"]
            mean_lon+=entry["lon"]

        mean_lon/=len(chart_data)
        mean_lat/=len(chart_data)

        print(mean_lat, mean_lon)

        st.pydeck_chart(pdk.Deck(
            # map_style=None,
            initial_view_state=pdk.ViewState(
                latitude=mean_lat,
                longitude=mean_lon,
                zoom=5,
                pitch = 0
            ),
            layers=[
                pdk.Layer(
                    "GeoJsonLayer",
                    data=chart_data,
                    get_fill_color=[247, 230, 202],
                ),
                pdk.Layer(
                    "TextLayer",
                    data = chart_data,
                    pickable=True,
                    get_position="[lon, lat]",
                    get_text="stance",
                    get_size=36,
                    get_color=[146, 121, 81],
                    # get_angle=0,
                    # Note that string constants in pydeck are explicitly passed as strings
                    # This distinguishes them from columns in a data set
                    # get_text_anchor="middle",
                    # get_alignment_baseline="center",
                )
            ]
        ))
        
        # st.map(pd.DataFrame.from_dict(st.session_state["Loc_List"]))
                    
                            
        

