import streamlit as st
import pandas as pd
import numpy as np

st.set_page_config(page_title="NLP Models", page_icon=None, layout="wide", initial_sidebar_state="auto", menu_items=None)



st.header("Age Prediction")

title = st.text_input('Paste tweet contents here', 'I feel swindled every time I drink one')
st.write('Predicted age is:', int(np.random.uniform(low=10, high=50, size=(1,))))


