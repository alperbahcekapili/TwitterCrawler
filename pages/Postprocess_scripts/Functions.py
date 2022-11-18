import pandas as pd
import os 
from numpy import random
from matplotlib import pyplot as plt


def predict_gender(names_df, name):
    
    name_set = names_df.iloc[:, 1]
    detected_names = []
    index = 0
    for base in name_set:
        if base.lower() in name.lower(): 
            detected_names.append(index)
        index+=1

    male_count = 0
    female_count = 0

    for index in detected_names:
        if "E" in names_df.iloc[index, 2]:
            male_count+=1
        else:
            female_count+=1
    
    if male_count > female_count:
        return "male"
    elif male_count < female_count:
        return "female"
    else:
        return "unknown"

import numpy as np


def predict_stance(usertext):

    selection = random.randint(0, 3)
    if selection == 0:
        return "AKP"
    if selection == 1:
        return "CHP"
    if selection == 2:
        return "IYI PARTI"
 

def predict_age(text): 
    random.seed(np.sum([ord(a) for a in text]))
    return random.randint(12, 84)


def get_age_interval(interval = 10, age = -1): 
    return f"{int(age/interval)*interval}-{(int(age/interval)*interval)+interval}"



import matplotlib.pyplot as pltbase
import numpy as np 

def have_similar(l, n):

    mindif = 100000
    for k in l:
        if abs(k - n) < mindif:
            mindif = abs(k - n)
    if mindif < 0.01:
        return True
    return False
    


def generate_figure(stats):


    print("Generating figures for: ")
    print(stats)


    keys = stats.keys()
    plt.clf()
    plt.close("all")

    # Pie chart, where the slices will be ordered and plotted counter-clockwise:
    labels = list(keys)
    sizes = [stats[k] for k in keys]

    total = sum(sizes)

    for i in range(len(labels)):
        labels[i] += "\n" + "%{perc:d}".format(perc = int(100 * float(sizes[i])/total))

    
    #explode = [1 if count == max(sizes) else 0 for count in sizes]  # only "explode" the 2nd slice (i.e. 'Hogs')

    fig1, ax1 = plt.subplots(figsize=(3,3))
    ax1.cla()
    # colors = ['#ff9999','#66b3ff','#99ff99']


    wedges, texts = ax1.pie(sizes, wedgeprops=dict(width=0.5), startangle=-40 )
    ax1.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.
    ylims = ax1.get_ylim()
    xlims = ax1.get_xlim()


    bbox_props = dict(boxstyle="square,pad=0.3", fc="w", ec="k", lw=0.72)
    kw = dict(arrowprops=dict(arrowstyle="-"),bbox=bbox_props, zorder=0, va="center")


    
    prev_angles = []



    for i, p in enumerate(wedges):
        ang = (p.theta2 - p.theta1)/2. + p.theta1

        if ang == 0 or ang == 180: 
            ang += 0.001

        y = np.sin(np.deg2rad(ang))
        x = np.cos(np.deg2rad(ang))

        texty = 1.4*y
        while have_similar(prev_angles, texty):
            texty += 0.01
        prev_angles.append(texty)

        horizontalalignment = {-1: "right", 1: "left"}[int(np.sign(x))]
        connectionstyle = "angle,angleA=0,angleB={}".format(ang)
        kw["arrowprops"].update({"connectionstyle": connectionstyle})
        
        ax1.annotate(labels[i], xy=(x, y), xytext=(1.35*np.sign(x), texty), horizontalalignment=horizontalalignment, **kw)


    centre_circle = plt.Circle((0,0),0.70,fc='white')
    fig = plt.gcf()
    fig.gca().add_artist(centre_circle)

    return fig1, ax1

import streamlit as st

     

def read_stances(file_path):
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

    return stances

def get_recursive_file_list(root_folder):
    file_list =  []
    for path, currentDirectory, files in os.walk(root_folder):
        for file in files:
            file_list.append(os.path.join(path, file))
    return file_list

import redis

def get_redis_client(port = 6379, dbno = 0, host = "localhost"):
    return redis.Redis(host=host, port=port, db=dbno)



def is_retweet_processed(tweet):
    # input: pd.Dataframe row

    # 3rd index retweet
    # 4th index RT @b33chichi: In honor of chp 402.1 bc this was literally him,

    if tweet.iloc[3] == "retweet" or "RT @" in tweet.iloc[4]:
        return tweet.iloc[4].split("@")[1].split(":")[0]
    
    return None
