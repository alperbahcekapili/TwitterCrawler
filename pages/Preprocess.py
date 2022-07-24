import streamlit as st
import pandas as pd

import time  # to simulate a real time data, time loop

import numpy as np  # np mean, np random
import plotly.express as px  # interactive charts



st.header("Preprocess")
st.write("You can preprocess the tweet you downloaded. You need to upload these tweets as bulk files. Here you will recieve visualizations of the statistics about tweets.")


def preprocess_text(text):
    text = text.replace("\n", ' ').replace("\r", " ").replace("\t", " ").replace("  "," ").strip()
    return text


def preprocess_tweet(tweet):

    tweet_dict = {}
    
    tweet_dict["time"] = tweet["created_at"]
    tweet_dict["ref"] = tweet["referenced_tweets"]
    tweet_dict["text"] = preprocess_text(tweet["text"])
    tweet_dict["id"] = tweet["id"]

    return tweet_dict