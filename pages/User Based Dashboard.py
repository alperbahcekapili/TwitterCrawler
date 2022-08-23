import streamlit as st
import json
import random
import os

st.set_page_config(page_title="User Based Dashboard", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)


st.title("User Based Dashboard")

dump_path = st.text_input("Give dump path to generate user dashboards...")
# here each line contains a tweet object

user_stats = {}
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

                if userid not in users:
                    users.add(userid)
                    user_stats[userid] = {"id": userid, "username":username}
                

            index+=1




    st.write("To inspect a user, select from the following list")

    names = ["James",
    "Robert",
    "John",
    "Michael",
    "David",
    "William",
    "Richard",
    "Joseph",
    "Thomas",
    "Charles",
    "Christopher",
    "Daniel",
    "Matthew",
    "Anthony",
    "Mark",
    "Donald",
    "Steven",
    "Paul",
    "Andrew",
    "Joshua",
    "Kenneth",
    "Kevin",
    "Brian",
    "George",
    "Timothy",
    "Ronald",
    "Edward",
    "Jason",
    "Jeffrey",
    "Ryan",
    "Jacob",
    "Gary",
    "Nicholas",
    "Eric",
    "Jonathan",
    ]




    user_stats_container = st.empty()



    def update_stats(selected_user):
        if selected_user == "":
            return
        # lokasyon, stance, yaş, cinsiyet
        cols = st.columns(6) 
        field_labels = ["Id", "Username", "Location", "Stance", "Age", "Sex"]
        fields = ["id", "username"]

        random_choices= [["Ankara", "Yalova", "Adana"], 
        ["Positive", "Very Positive","Negative", "Very Negative"],
        ["10", "12", "23", "59", "42", "34", "51"],
        ["Male", "Female"]]



        index = 0
        for col in cols:    
            with col:
                st.subheader(field_labels[index])
                if index < 2:    
                    st.write(user_stats[selected_user][fields[index]])
                else:
                    st.write(random_choices[index-2][random.randint(0,len(random_choices[index-2])-1)])
                
            index +=1
    selected_user = st.selectbox("Users",users)
    if selected_user:
        update_stats(selected_user)

    
