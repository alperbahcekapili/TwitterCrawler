import ast
import json
from st_aggrid import GridOptionsBuilder, AgGrid, GridUpdateMode, DataReturnMode
from pages.Postprocess_scripts.Functions import get_recursive_file_list
from pages.Preprocess_scripts.Functions import get_line_count, process_tweet, zip_open
import streamlit as st
import gzip
import pandas as pd
from zipfile import ZipFile

def preprocess_text(text):
    text = text.replace("\n", ' ').replace("\r", " ").replace("\t", " ").replace("  "," ").strip().encode("utf-8").decode("utf-8")
    return text

def sum_mask_numpy(value):
    user_stats = st.session_state["user_stats"] 
    return user_stats["usertext"].map(lambda x : value in x).count()

def do_analysis(labels):


    counts = {}
    for label in list(labels.keys()):
        counts[label] = 0
        for name in labels[label]:
            count = sum_mask_numpy(name)
            counts[label] += count
    return counts




users = set()
line_count = 0
file_count = 0
user_data = {}
root = st.text_input("Please write root folder of the tweets...")
load_data = st.button("Load Data")
if root and load_data:
    st.text("Starting the processing step")
    progress_bar = st.empty()
    to_be_processed_file_list= get_recursive_file_list(root)
    index = 0
    for index in range(len(to_be_processed_file_list)):      
        dump_file = None
        if   to_be_processed_file_list[index].endswith("gzip"):
            dump_file = gzip.open(to_be_processed_file_list[index], "rt")
        elif to_be_processed_file_list[index].endswith("zip"):
            dump_file = zip_open(to_be_processed_file_list[index])
        if dump_file is None:
            raise Exception("File format not accepted...")
        
        with dump_file:
            # Get next line from filer
            total_lines = get_line_count(to_be_processed_file_list[index])
            file_count += 1
            for line in dump_file:
                line_count += 1
                if line_count % 100 == 0:
                    #change here
                    break
                    with progress_bar.container():
                        st.text("Tweets read: " + str(line_count) + f": {total_lines}, total files opened:  "+ str(file_count) + f":{len(to_be_processed_file_list)}") 
                print(line)
                tweet = None
                try:
                    tweet = ast.literal_eval(line)
                except Exception as e:
                    print("A problem occured during loading following line: \n")
                    print(line)
                    print(e)
                
                if tweet is None: 
                    continue
                try:
                    result = process_tweet(tweet ,option="Dump", names= pd.DataFrame(), r = "")
                except Exception as e:
                    print("A problem occured during processing following tweet...\n" + str(tweet))
                    print(e)
                    continue
                [userid, username, usertext ,userloc, userstance, userage, predicted_gender] = result
                if userid not in user_data:
                    user_data[userid] =[userid, username, {"texts":[preprocess_text(usertext)]},userloc, userstance, userage, predicted_gender]
                else:
                    user_data[userid][2]["texts"].append(preprocess_text(usertext))
    st.session_state["user_data"] = user_data
    st.session_state["total_tweets"] = line_count
    st.session_state["total_users"] = len(user_data)


reload = st.button("Show Data")
if reload:
    user_stats = pd.DataFrame.from_dict(st.session_state["user_data"], orient="index", columns = ["userid", "username", "usertext","userloc", "userstance", "userage", "predicted_gender"])
    user_stats["usertext"] = user_stats["usertext"].map(lambda x: json.dumps(x["texts"]))
    st.session_state["user_stats"] = user_stats
    
    gb = GridOptionsBuilder.from_dataframe(user_stats)
    gb.configure_pagination(paginationAutoPageSize=True) #Add pagination
    gb.configure_side_bar() #Add a sidebar
    #gb.configure_selection('multiple', use_checkbox=True, groupSelectsChildren="Group checkbox select children") #Enable multi-row selection
    gridOptions = gb.build()
    grid_response = AgGrid(
                user_stats,
                gridOptions=gridOptions,
                data_return_mode='AS_INPUT', 
                update_mode='MANUAL', 
                fit_columns_on_grid_load=False,
                theme='material', #Add theme color to the table
                enable_enterprise_modules=True,
                height=500, 
                width='100%',
                reload_data=False
            )
    
analyze = st.button("Start Analyze")
if analyze or st.session_state["basic_analyze"]:
    st.session_state["basic_analyze"] = True
    st.text("Analyzed")
    user_stats = st.session_state["user_stats"] 
    
    # visualize labels
    labels_file = st.text_input("Please give labels...")
    if labels_file:
        labels_pr = json.load(open(labels_file, "r"))        
        labels  = {}
        for el in labels_pr:
            labels[list(el.keys())[0]] = el[list(el.keys())[0] ]
        st.write(labels)
    
        # now we can do the analyses
        counts = do_analysis(labels)
        st.write(counts)