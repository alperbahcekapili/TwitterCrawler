import streamlit as st
import pandas as pd













def calculate_support_ratios(dated_electorants):


    
    loc_detected_rows = st.session_state["user_stats"][st.session_state["user_stats"]["Location Detected"] == "+"]
    loc_detected_rows = loc_detected_rows[loc_detected_rows["Stance Detected"] == "+"]
    support_values  = {}
    state_counts = {} # {state1: #users, ...}


    state_tresh = 1

    total_electorates = dated_electorants.iloc[1:].sum()
    # extract locs and stances
    locs = set()
    stances = set()

    index = 0
    for index, row in loc_detected_rows.iterrows(): 
        index=1
        stat = row
        loc = stat["Userloc"]
        stance = stat["Stance"]

        if stat["Stance"] is not None:
            stances.add(stat["Stance"])

        if stat["Userloc"] is not None:
            locs.add(stat["Userloc"])
                
            if loc not in state_counts:
                state_counts[loc] = 0

            state_counts[stat["Userloc"]] += 1

        if stat["Userloc"] is not None and stat["Stance"] is not None:
            # check loc is in supportvalues
            if loc not in support_values:
                support_values[loc] = {}
            
            # now check stance in support_values["loc"]

            if stance not in support_values[loc]:
                support_values[loc][stance] = 0


            support_values[loc][stance]+=1

    for col in list(support_values.keys()):
        for stance in list(support_values[col].keys()) :
            st.write(f"{col}, {stance} user counts: {support_values[col][stance]}")

    # now we have invidiual support values for locations and stances
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
    return support_ratios, support_values





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


