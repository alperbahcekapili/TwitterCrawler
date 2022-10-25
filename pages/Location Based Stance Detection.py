import streamlit as st
import pandas as pd


# /home/alper/Documents/Twitter Crawler Documents/GermanyElections/Voters.csv
electorants =  st.text_input("Please give the total electorants file")

# /home/alper/Documents/Twitter Crawler Documents/updated_locations_german_elections.csv
locations = st.text_input("Please give the user-location file")


# /home/alper/Documents/Twitter Crawler Documents/it25-stance-users.json
stances = st.text_input("Please give the user-stance file")

# /home/alper/Downloads/user-id_dict.csv
user_id_dict = st.text_input("Please provide the user-id dictionary file path")

  

userids = set()

users = {}
username2id = {}



def get_dated_electorants(path):
    electorants = pd.read_csv(path, lineterminator="\n")
    st.dataframe(electorants)
    selected_row =  st.number_input("Please select which row you want to use", 0, len(electorants)-1)

    electorants_date = electorants.iloc[selected_row, :]
    return electorants_date


import time

def get_user_id_dictionary(path):
    useriddict = pd.read_csv(path)

    start_time = time.time()

    stats_container = st.empty()

    for index, row in useriddict.iterrows():

        if index%10000 == 0:
           with stats_container.container():
                st.write(f"Parsed {len(userids)} users in {time.time() - start_time} seconds")
        


        name = row.iloc[2]
        id = row.iloc[1]
        

        if id not in userids:
            userids.add(id)
            users[id] = {
                "name" : name,
                "id" :  id,
                "loc" : None,
                "stance": None 
            }
            username2id[name] = id


    #json.dump(users, open("ultimate_stats.json", "w"))

    with stats_container.container():
        st.write(f"Parsed {len(userids)} users in {time.time() - start_time} seconds")
        

def parse_locations(path):
    locations = pd.read_csv(path)
    error_count =0
    for index, row in locations.iterrows():
        loc = row.iloc[2]
        id = row.iloc[1]
    
        try:
            users[id]["loc"] = loc
        except :
            error_count+=1
            print("Errors in parse locations", error_count)
            continue


        
        # should not throw error if id exist in id-name dictionary

    return False
import json

def parse_stances(path):
    error_count = 0
    stances_list =  json.load(open(path, "r"))
    for stance_entry in stances_list.keys():
        for username_entry in stances_list[stance_entry]:
            try:
                temp_id = username2id[username_entry]
                users[temp_id]["stance"] = stance_entry
            except:
                error_count+=1
                print("Errors in parse stances", error_count)

    return False



def extract_stats():

    total_users = len(userids)
    users_with_locations = 0
    users_with_stances = 0
    users_with_stances_n_locations = 0
    users_without_stance_nor_locations = 0


    st.write("Extracting stats...")


    index = 0

    stats_container = st.empty()

    for user in users.keys():
        index+=1

        if index %  1000 == 0:
            with stats_container.container():

                st.write(f"Total Users: {total_users}")
                st.write(f"users_with_stances_n_locations: {users_with_stances_n_locations}")
                st.write(f"users_with_locations: {users_with_locations}")
                st.write(f"users_with_stances: {users_with_stances}")
                st.write(f"users_without_stance_nor_locations: {users_without_stance_nor_locations}")
                st.write(f"After looking at {index} users")



        
        entry = users[user]
        if entry["stance"] == None and entry["loc"] == None:
            users_without_stance_nor_locations+=1
        elif entry["stance"] == None: 
            users_with_locations +=1
        elif entry["loc"] == None: 
            users_with_stances +=1
        else:
            users_with_stances_n_locations+=1


    with stats_container.container():

        st.write(f"Total Users: {total_users}")
        st.write(f"users_with_stances_n_locations: {users_with_stances_n_locations}")
        st.write(f"users_with_locations: {users_with_locations}")
        st.write(f"users_with_stances: {users_with_stances}")
        st.write(f"users_without_stance_nor_locations: {users_without_stance_nor_locations}")
        st.write(f"After looking at {index} users")


        
    temp_stats = {
            "total_users": users_with_stances_n_locations, 
            "users_with_stances_n_locations": users_with_stances_n_locations, 
            "users_with_locations": users_with_locations,
            "users_with_stances": users_with_stances,
            "users_without_stance_nor_locations": users_without_stance_nor_locations
        }
    json.dump(temp_stats, open("new_stats_summary", "w"))


all_given = electorants and locations and stances and user_id_dict




do_stats_exist = st.multiselect("Does the statistics saved before ?", [False, True])



if all_given:
    # read electorant stats and print them

    dated_electorants = get_dated_electorants("/home/alper/Documents/Twitter Crawler Documents/GermanyElections/Voters.csv")

    # initialize users
    st.write("Generating user id dictionary")
    get_user_id_dictionary(user_id_dict)


    # parse locations and stances
    st.write("Parsing Locations")
    parse_locations(locations)

    st.write("Parsing Stances")
    parse_stances(stances)


    #extract_stats()
    # now all informations are available in one dictionary



    support_values  = {}
    state_counts = {}


    state_tresh = 1000

    total_electorates = dated_electorants.iloc[1:].sum()



    # extract locs and stances
    locs = set()
    stances = set()

    index = 0
    for stat in users.keys(): 


        index=1
        stat = users[stat]

        loc = stat["loc"]
        stance = stat["stance"]

        if stat["stance"] is not None:
            stances.add(stat["stance"])

        if stat["loc"] is not None:
            locs.add(stat["loc"])
            
            
            if loc not in state_counts:
                state_counts[loc] = 0

            state_counts[stat["loc"]] += 1


        if stat["loc"] is not None and stat["stance"] is not None:
            # check loc is in supportvalues

            

            if loc not in support_values:
                support_values[loc] = {}
            
            # now check stance in support_values["loc"]

            if not stance in support_values[loc]:
                support_values[loc][stance] = 0


            support_values[loc][stance]+=1

                
        

    


    for col in list(support_values.keys()):
        for stance in list(support_values[col].keys()) :
            st.write(f"{col}, {stance} user counts: {support_values[col][stance]}")

   # now we have invidiual support values for locations and stances
    # 



    # now lets calculate actual support ratios

    support_ratios = {}

    for stance in list(stances) :

        top = 0
        bottom  = 0

        for loc in list(support_values.keys()):

            # g() function
            if state_counts[loc] < state_tresh:
                continue
            
            nic = support_values[loc][stance]
            ni = state_counts[loc]

            ei = dated_electorants[loc]
            eic = total_electorates



            top+= nic * float(ei)/float(eic)
            bottom+= ni * float(ei)/float(eic)


        support_ratios[stance] = top/bottom





    st.write(support_ratios)


 









