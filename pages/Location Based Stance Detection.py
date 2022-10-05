import streamlit as st
import pandas as pd


# /home/alper/Documents/Twitter Crawler Documents/GermanyElections/Voters.csv
electorants =  st.text_input("Please give the total electorants file")

# /home/alper/TwitterCrawler/only_detected.csv
locations = st.text_input("Please give the user-location file")


# /home/alper/Documents/Twitter Crawler Documents/it25-stance-users.json
stances = st.text_input("Please give the user-stance file")



# import the module
import tweepy
  



def get_dated_electorants(path):
    electorants = pd.read_csv(path, lineterminator="\n")
    st.dataframe(electorants)
    selected_row =  st.number_input("Please select which row you want to use", 0, len(electorants)-1)
    electorants_date = electorants.iloc[selected_row, :]
    return electorants_date

def parse_locations(path):
    return False


all_given = electorants and locations and stances
if all_given:
    # read electorant stats and print them

    dated_electorant = get_dated_electorants("/home/alper/Documents/Twitter Crawler Documents/GermanyElections/Voters.csv")
    

