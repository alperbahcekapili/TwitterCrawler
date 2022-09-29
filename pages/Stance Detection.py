from time import sleep
import streamlit as st
import pandas as pd
import json
from pages.Postprocess_scripts.Stance_Detection import StanceDetection
from multiprocessing import Process, Queue


st.header("Stance Detection")
st.title("Retweet based stance detection")

st.write("Please give a file that has the following form: (stance names should not contain '/')")
code = """{stance}
{twitter_profile}
{twitter_profile}
{twitter_profile}"""
st.code(code)


file_path = st.text_input("Stance labels file")
if file_path:
    stances = {}
    file = open(file_path, "r")

    latest = ""

    for line in file.readlines():
        if "/" not in line :
            latest = line.rstrip().lstrip()
            stances[latest] = []
        elif "\n" != line and "" != line and "\r" != line:
            username = line.split("/")[-1].rstrip()
            stances[latest].append(username)


    st.write(stances)
    tweets_file_path =  st.text_input("Please provide the root folder that contains processed csv's")
    
    if tweets_file_path:
        st.info("Starting Process...")
        st.session_state["stats_queue"] = Queue(1000)
        st.session_state["preprocess_process"] =  Process(target=StanceDetection, args=(tweets_file_path, stances, st.session_state["stats_queue"] ))
        st.session_state["preprocess_process"].start()


        stats = st.empty()

        while(True):

            
            sleep(0.001)
            if not st.session_state["stats_queue"].empty():
                response = st.session_state["stats_queue"].get(block=True, timeout=1)

                if "break" in response.keys():
                    break


                with stats.container():
                    st.write(response)


    
