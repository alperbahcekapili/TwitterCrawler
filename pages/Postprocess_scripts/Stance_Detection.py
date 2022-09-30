import os
from multiprocessing import Process, Queue
import signal
import redis 
import pandas as pd
import json
import copy
from datetime import datetime
import time

class StanceDetection:

    # unique userids
    users = []
    # retweeted user lists
    retweeted = []
    # stances for each user
    label = []



    stance_user_dict = {}
    



    def get_to_be_processed_files(self, processed_path):
        current_folder = processed_path
        to_be_processed_file_list = os.listdir(current_folder)
        templist = []
        for i in range(len(to_be_processed_file_list)):
                templist.append(os.path.join(current_folder , to_be_processed_file_list[i]))
        
        to_be_processed_file_list = templist
        index = 0

        while len(to_be_processed_file_list) >  index:
            isDir = os.path.isdir(to_be_processed_file_list[index])
            if isDir: 
                new_file_list = os.listdir(to_be_processed_file_list[index])
                to_be_appended = list(map(lambda x  : os.path.join(to_be_processed_file_list[index], x), new_file_list))
                to_be_processed_file_list.extend(to_be_appended)
                to_be_processed_file_list.remove(to_be_processed_file_list[index])    

            else:
                index+=1
    
        return to_be_processed_file_list

    

    def is_master(self, username):
        for stance in self.stances.keys():
            if username in self.stances[stance]:
                return stance
        return False

    def extract_users_retweets(self, dataframe):
        
        for index, row in dataframe.iterrows():

            reftype = row["ref_type"]
            text = row["text"]

            if reftype != "retweet" or "RT" not in text:
                continue

            referred_user = text.split("@")[1].split(":")[0]


            username = row["user_screen_name"]
            

            """
            Master Users Control
            if master user is faced then we should not make any changes
            """

            if self.is_master(username) != False:
                continue

            

            if username not in self.users:
                self.users.append(username)
                self.retweeted.append([referred_user])
                self.label.append("Unknown")
            else:
                # we saw this user before now check if this retweeted user is saw before
                temp_index = self.users.index(username)
                temp_retweeted_users = self.retweeted[temp_index]
                if referred_user in temp_retweeted_users:
                    continue

                self.retweeted[temp_index].append(referred_user)



        json.dump({"users": self.users, "users_retweeted": self.retweeted}  ,open("retweets-users.json", "w"))

    def one_iteration(self):
        # iterate through all users and change immedietly if needed

        prev_stats = copy.deepcopy(self.stance_stats)
        changed_users = 0 

        for i in range(len(self.users)):

            """
            Master Users Control
            if master user is faced then we should not make any changes
            """

            if self.is_master(self.users[i]) != False:
                continue

           



            # get current stance of that user
            current_stance = self.label[i]

            # we will iterate through the users current user retweeted
            # we will add the stances we obtain from retweeted users
            # if only one is detected then we will set this as stance of the user
            # otherwise we will set label as Unknown
            detected_stances = set()
            for retweeted_user in self.retweeted[i]:
                for stance in self.stance_user_dict.keys():
                    if retweeted_user in self.stance_user_dict[stance]:
                        # if retweeted user in our previously labeled users then we will add to detected_stances
                        detected_stances.add(stance)
                        # we can break because each user can be attached to only one stance
                        break

            
            if len(detected_stances) == 1:
                # now we can set this user as stance as well
                to_set_stance = detected_stances.pop()
                if to_set_stance != current_stance:
                    changed_users += 1


                # change older state
                if current_stance != "Unknown":
                    self.stance_user_dict[current_stance].remove(self.users[i])

                # add to set
                self.stance_user_dict[to_set_stance].add(self.users[i])
                # set individual stance
                self.label[i] = to_set_stance
            # then we should set label as unknown


            else:

                # change older state if was not already onknown
                if current_stance != "Unknown":
                    self.stance_user_dict[current_stance].remove(self.users[i])
                    self.label[i] = "Unknown"

        return changed_users

    def load_preexisting_dict(self, dict_path):
        user_retweets =  json.load(open(dict_path, "r"))
        self.users = user_retweets["users"]
        self.retweeted = user_retweets["users_retweeted"]
        self.label = ["Unknown" for i in range(len(self.users))]

        # set master users

        for stance in self.stances.keys():
            self.stance_stats[stance] = 0
            self.stance_user_dict[stance] = set()

            for username in self.stances[stance]:
                self.stance_user_dict[stance].add(username)
                self.label[self.users.index(username)] = stance

    def __init__(self, csv_root_path, stances_object, comm_queue, from_preexisting):
        self.comm_queue = comm_queue
        self.stances = stances_object

        # create variables for stance statistics 
        self.stance_stats = {}
        

        
        
        if not from_preexisting:
            # add master users to the list
            for stance in self.stances.keys():
                self.stance_stats[stance] = 0
                self.stance_user_dict[stance] = set()

                for username in self.stances[stance]:
                    self.stance_user_dict[stance].add(username)
                    self.users.append(username)
                    self.retweeted.append([])
                    self.label.append(stance)
            all_files = self.get_to_be_processed_files(csv_root_path)

            for file_path in all_files:

                self.extract_users_retweets(pd.read_csv(file_path, lineterminator="\n"))
                self.comm_queue.put({"retweeted_user":len(self.users)})

            
            self.comm_queue.put({"retweeted_user":len(self.users), "break":True})

        # parse all to get retweeted lists
        else:
            # this parameter can be changed. it is not csv root path in this case
            print("Loading from preexisting user-retweeted_users json file ")
            self.load_preexisting_dict(csv_root_path)
            self.comm_queue.put({"retweeted_user":len(self.users), "break":True})

        iteration = 0

        # start iterations
        changed_users = 1001
        while(changed_users > 1000):
            print(f"Starting iteration number: {iteration}")
            iteration+=1
            print("Start time: ", datetime.now())    
            start_time = time.time()
            changed_users = self.one_iteration()
            print("Epoch execution time: ", time.time() - start_time)
            print("Total stance changes: ",changed_users)
        

            to_save_dict = {}

            for stance in self.stance_user_dict.keys():
                print(f"User count for {stance} stance: {len(self.stance_user_dict[stance])}")
                to_save_dict[stance] = list(self.stance_user_dict[stance])
                self.stance_stats[stance]  = len(self.stance_user_dict[stance])






            json.dump(to_save_dict, open(f"it{iteration}-stance-users.json", "w"))
            json.dump(self.stance_stats, open(f"it{iteration}-stance-stats.json" , "w"))



        










    



