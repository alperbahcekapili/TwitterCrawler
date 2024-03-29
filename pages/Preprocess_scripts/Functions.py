
import gzip
from io import TextIOWrapper
from multiprocessing import Process, Queue
from zipfile import ZipFile
from pages.Postprocess_scripts.Functions import generate_figure, get_redis_client, predict_age, predict_gender, get_recursive_file_list, predict_stance
import streamlit as st
import random
import os
import redis
import time
import pandas as pd
from redis.commands.json.path import Path
import sys
sys.path.insert(1, os.path.join(os.getcwd(), 'pages/Postprocess_scripts'))

from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode


def zip_open(filename):
    """Wrapper function that for zipfiles."""
    with ZipFile(filename) as zipfin:
        for filename in zipfin.namelist():
            return TextIOWrapper(zipfin.open(filename))

def get_line_count(file_path):
    if file_path.endswith("gzip"):
        dump_file = gzip.open(file_path, "rt")
    elif file_path.endswith("zip"):
        dump_file = zip_open(file_path)
    else:
        dump_file = open(file_path, "r")
    return len(dump_file.readlines())

    

def process_tweet(row, option: str, names: list, r ):

    if option == "Processed":
        username = row["user_screen_name"]
        userid = row["user_id"]
        userloc = row["user_location"]


        # update gender statistics
        predicted_gender = predict_gender(names ,username)

        
        # text
        usertext = row["text"]

        # update stance statistics
        userstance = predict_stance(usertext)

        # age  
        userage = predict_age(usertext)


    elif option == "Dump":
        author = row["includes"]["users"][0]
        username = author["username"]
        userid = author["id"]
        if "location" in author:
            userloc = author["location"]
        else:
            userloc = ""

        # update gender statistics
        #predicted_gender = predict_gender(names ,username)        
        predicted_gender = "unknown"
        # text
        usertext = row["data"]["text"]

        # update stance statistics
        userstance = predict_stance(usertext)

        # age  
        userage = predict_age(usertext)


        # if 0 == r.exists(userid):
        #     r.set(str(userid), row["text"])
        # else:
        #     r.set(str(userid), "\n".join((r.get(userid).decode("utf-8"),row["text"])))


    else:

        raise Exception("Options should be either dump or processed. There is a problem...")


    # [userid, username, usertext ,userloc, userstance, userage, predicted_gender]

    return [userid ,username, usertext ,userloc, userstance, userage, predicted_gender]





def generate_abrs_object(abrs_path):
        locations_file = open(abrs_path, "r")
        locations = {}
        all_abbrs = ""
        for line in locations_file.readlines():
            line_slpitted1 = line.split(":")
            base_location = line_slpitted1[0].lower()
            line_slpitted2 = line_slpitted1[1].split(",")
            locations[base_location] =  list(map(lambda x: x.lower(), line_slpitted2))
            for abr in line_slpitted2:
                all_abbrs+=","+abr.lower()

        return locations


def try_new_locations(abrs):

    detected_users = []
    
    for index, user in st.session_state["user_stats"].iterrows():

        user_loc = user["Userloc"]
        user_loc = user_loc.lower()
        did_detect = False

        # iterate over all locaitons
        for base_loc in list(abrs.keys()):
            
            # if user location in abr base. ex. user loc: Los Angl, abr base: Los Angles or
            # it is explicitly in one of abrs list
            if user_loc in abrs[base_loc] or user_loc in base_loc or base_loc in user_loc:                        
                st.session_state["user_stats"].at[index,"Userloc"] = base_loc
                detected_users.append(index)
                break

            for abr_temp in abrs[base_loc]:
                if user_loc in abr_temp or abr_temp in user_loc:
                    st.session_state["user_stats"].at[index,"Userloc"] = base_loc
                    detected_users.append(index)
                    break
        
        print("Here location detection!!!!!!!")
        print(st.session_state["user_stats"]["Userloc"])
                
    return detected_users



from datetime import datetime
def visualize_results(statistics_pane, user_stats, predicted_stats):

    (stances, ages, genders) = predicted_stats

    with statistics_pane.container(): 


        gb = GridOptionsBuilder.from_dataframe(user_stats)
        gb.configure_pagination(paginationAutoPageSize=True) #Add pagination
        gb.configure_side_bar() #Add a sidebar
        #gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren="Group checkbox select children") #Enable multi-row selection
        gridOptions = gb.build()
        custom_css ={"--ag-widget-container-vertical-padding":"100px",
                     "--ag-cell-widget-spacing":"100px"}

        if "user_stats" not in st.session_state:

            grid_response = AgGrid(
                user_stats,
                gridOptions=gridOptions,
                data_return_mode='AS_INPUT', 
                update_mode='MODEL_CHANGED', 
                fit_columns_on_grid_load=False,
                theme='material', #Add theme color to the table
                enable_enterprise_modules=True,
                height=500, 
                width='100%',
                reload_data=True,
                key = str(datetime.now())
            )
        else:
            grid_response = AgGrid(
                user_stats,
                gridOptions=gridOptions,
                data_return_mode='AS_INPUT', 
                update_mode='MODEL_CHANGED', 
                fit_columns_on_grid_load=False,
                theme='material', #Add theme color to the table
                enable_enterprise_modules=True,
                height=500, 
                width='100%',
                reload_data=True
            )

        # data = grid_response['data']
        #selected = grid_response['selected_rows'] 
        #df = pd.DataFrame(selected) #Pass the selected rows to a new dataframe df


        

        col1, col2, col3  = st.columns(3, gap = "medium")

        with col1: 

            st.subheader("Age prediction")
            fig, ax = generate_figure(ages)
            st.pyplot(fig)
            

        with col2:

            st.subheader("Gender prediction")
            fig, ax = generate_figure(genders)
            st.pyplot(fig)

        with col3: 
            
            st.subheader("Stance prediction")
            fig, ax = generate_figure(stances)
            st.pyplot(fig)

    