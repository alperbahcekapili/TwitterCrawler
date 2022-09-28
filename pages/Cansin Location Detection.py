
from time import sleep
import streamlit as st
from pages.Postprocess_scripts.Cansin_Location_Detection import Cansin_Location_Detector
from multiprocessing import Process, Queue
import pandas as pd
import os

st.title("Cansin Location Detection")


csv_root_path =  st.text_input("Please give preprocessed dump path")
abrs_file_path = st.text_input("Please give abrs. file path")

if csv_root_path and abrs_file_path: 
    st.info("Starting the process...")

    st.session_state["stats_queue"] = Queue(1000)
    st.session_state["preprocess_process"] =  Process(target=Cansin_Location_Detector, args=(abrs_file_path, csv_root_path, st.session_state["stats_queue"] ))
    st.session_state["preprocess_process"].start()

    stat_view = st.empty()

    no_resp_count = 0

    sleep(3)

    updated_indexes = []

    while True: 

        if no_resp_count > 500:
            break

        
        sleep(0.001)
        if not st.session_state["stats_queue"].empty():
            
            

            response = st.session_state["stats_queue"].get(block=True, timeout=1)
            if "updated_indexes" in response.keys():
                updated_indexes = response["updated_indexes"]
            no_resp_count = 0
            with stat_view.container():
                
                
                # stats = {
                #     "total_users": self.detected_users + self.undetected_users,
                #     "detected_users": self.detected_users,
                #     "detection_rate": self.detection_rate 
                # }


                st.write(response)

        else:
            no_resp_count +=1


    results =  pd.read_csv("Updated_Locations.csv" , lineterminator="\n" )
    st.dataframe(results)

    if updated_indexes != []:
        updated_loc = pd.read_csv("only_detected.csv", lineterminator="\n")
        st.dataframe(updated_loc)
            
