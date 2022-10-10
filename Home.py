import streamlit as st
import pandas as pd

st.set_page_config(page_title="Home", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)



st.title('Welcome')

st.write("This tool is designed to assist researchers works with Twitter API and tweets."+
"Tool has several features. Below you can find informations about each one of them. ")

expander = st.expander("Instructions")

readme_file = open("Readme.md")
readme_str =""
for line in readme_file.readlines():
    readme_str += line 
expander.write(readme_str)
