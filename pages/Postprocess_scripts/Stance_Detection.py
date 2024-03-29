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
    # retweet counts for each user
    label = []

    # user-stance
    stances = {}



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

            # means they have been read from csv
            if "data" in row:
                text = row["data"]["text"]
                if  "RT" not in text:
                    continue
                referred_user = text.split("@")[1].split(":")[0]
                username = row["user_screen_name"]
            # means they come from user_stats list from st.session_state
            elif "Username" in row:
                text = row["Text"]
                if  "RT" not in text:
                    continue
                referred_user = ""
                try:
                    referred_user = text.split("@")[1].split(":")[0]
                except Exception as e:
                    continue
                username = row["Username"]


            """
            Master Users Control
            if master user is faced then we should not make any changes
            """
            print(self.stances)
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


        print("In extract user retweets")
        print(self.users, self.retweeted)
        import os
        json.dump({"users": self.users, "users_retweeted": self.retweeted},open(os.path.join("local","retweets-users.json"), "w"))

    def one_iteration(self, warmup=False):
        # iterate through all users and change immedietly if needed

        prev_stats = copy.deepcopy(self.stance_stats)
        changed_users = 0 


        print(self.users)
        print(self.label)
        print(self.stance_user_dict)
        print(self.stance_stats)
        print(self.stances)
        print(self.user_stances)
        print(self.retweeted)

        for i in range(len(self.users)):

            """
            Master Users Control
            if master user is faced then we should not make any changes
            """

            if self.is_master(self.users[i]):
                continue

            # get current stance of that user
            current_stance = self.label[i]

            # we will iterate through the users current user retweeted
            # we will add the stances we obtain from retweeted users
            # if max voted stance is threshold bigger than others then assign this stance
            # otherwise we will set label as Unknown
            detected_stances = set()
            detected_stance_counts = {}

            # reset stance counts
            for stance in self.stance_user_dict.keys():
                detected_stance_counts[stance] = 0
            
            # update stance counts before this iteration
            for retweeted_user in self.retweeted[i]:
                if retweeted_user in self.user_stances and  self.user_stances[retweeted_user] != "Unknown":
                    detected_stance_counts[self.user_stances[retweeted_user]]+=1 
                    


            #print(detected_stance_counts)

            def getStance(detected_stance_counts, warmup=False):

                max= {"stance": "", "count": -1}
                second = {"stance": "", "count": -2}
                for k,v in detected_stance_counts.items():

                    
                    if max["count"]== -1:
                        max["stance"] = k
                        max["count"] = v
                        break
                index = 0


                for k,v in detected_stance_counts.items():
                    
                    index+=1
                    if index == 1:
                        continue
                    
                    if v > max["count"] and v > second["count"]:
                        second["stance"] = max["stance"]
                        second["count"] = max["count"]
                        max["count"] = v
                        max["stance"] = k
                    
                    elif v > second["count"]:
                        second["count"] = v
                        second["stance"] = k
                       
                if warmup:
                    return max["stance"]
                elif max["count"]-second["count"] >= 3     : # if warmup else 5
                    return max["stance"]
                
                return "Unknown"


            new_stance = getStance(detected_stance_counts, warmup)    

            if new_stance != current_stance:
                self.user_stances[self.users[i]] = new_stance


                changed_users += 1

                 # change older state
                if current_stance != "Unknown" and self.users[i] in self.stance_user_dict[current_stance]:
                    
                    self.stance_user_dict[current_stance].remove(self.users[i])
                    self.label[i] = "Unknown"


                if new_stance != "Unknown":
                        
                    self.stance_user_dict[new_stance].add(self.users[i])
                    # set individual stance
                    self.label[i] = new_stance

        print(self.users)
        print(self.label)
        print(self.stance_user_dict)
        print(self.stance_stats)
        print(self.stances)
        print(self.user_stances)
        print(self.retweeted)



        return changed_users

    def load_preexisting_dict(self, dict_path):
        user_retweets =  json.load(open(dict_path, "r"))
        self.users = user_retweets["users"]
        self.retweeted = user_retweets["users_retweeted"]
        self.label = ["Unknown" for i in range(len(self.users))]
        for user in self.users:
            self.user_stances[user] = "Unknown"
        



        # set master users

        for stance in self.stances.keys():
            self.stance_stats[stance] = 0
            self.stance_user_dict[stance] = set()

            for username in self.stances[stance]:
                self.stance_user_dict[stance].add(username)
                if username in self.users: 
                    # if we crawled data for this person then we don need to add new user to existing users
                    self.label[self.users.index(username)] = stance
                    self.user_stances[username] = stance
                else: 
                    # if this person is not in our users then we should extend lists
                    self.users.append(username)
                    self.label.append(stance)
                    self.user_stances[username] = stance
                    self.retweeted.append([])

    def __init__(self, user_stats, stances_object, comm_queue):
        self.comm_queue = comm_queue
        self.stances = stances_object

        # create variables for stance statistics 
        self.stance_stats = {}
        
        # user-stance
        self.user_stances = {}

        
        
        # add master users to the list
        for stance in self.stances.keys():
            self.stance_stats[stance] = 0
            self.stance_user_dict[stance] = set()

            for username in self.stances[stance]:
                self.stance_user_dict[stance].add(username)
                self.users.append(username)
                self.retweeted.append([])
                self.user_stances[username] = stance
                self.label.append(stance)
            

            self.extract_users_retweets(user_stats)
            self.comm_queue.put({"retweeted_user":len(self.users)})

        
        print(self.users)
        print(self.retweeted)

       
        iteration = 0

        # start iterations
        changed_users = 1001
        while(changed_users > 3 and iteration > 5):
            print(f"Starting iteration number: {iteration}")
            iteration+=1
            print("Start time: ", datetime.now())    
            start_time = time.time()
            changed_users = self.one_iteration(warmup=iteration<5)
            print("Epoch execution time: ", time.time() - start_time)
            print("Total stance changes: ",changed_users)

            to_save_dict = {}

            for stance in self.stance_user_dict.keys():
                print(f"User count for {stance} stance: {len(self.stance_user_dict[stance])}")
                to_save_dict[stance] = list(self.stance_user_dict[stance])
                self.stance_stats[stance]  = len(self.stance_user_dict[stance])

            dir = "iterations"
            if not os.path.exists(dir):
                os.makedirs(dir)

            json.dump(to_save_dict, open(os.path.join(dir,f"it{iteration}-stance-users.json"), "w"))
            json.dump(self.stance_stats, open(os.path.join(dir,f"it{iteration}-stance-stats.json") , "w"))

        
        # here put final stances to pipe
        self.comm_queue.put({"user_stance_dict": self.user_stances})
        self.comm_queue.put({"stance_user_dict":self.stance_user_dict})
        self.comm_queue.put({"retweeted_user":len(self.users), "break":True})
        










    



