
"""

Get needed information from Streamlit UI

Get abrs file path, csv root path

Returns:
Newly generated locations: 
2 lists: 1 user ids, 1 new_locaitons
these are indexed in a way so the person and locations are matching
statistics:
total_user_counts
detected_locations: again 2 lists with detections count
total detection percentages


"""

import os
from multiprocessing import Process, Queue
import signal
import redis 
import pandas as pd
import json


class Cansin_Location_Detector: 

    kill_now = False
    
    detected_users = 0
    undetected_users = 0
    detection_rate = 0

    updated_indexes = []
    users = []
    usernames = []
    locations = []

    
    # share same index for same person


    r = redis.Redis(host='localhost', port=6379, db=0)


    def exit_gracefully(self, *args):
        self.kill_now = True



    # assumes all abrs to be unique ideftifier for the base
    def generate_abrs_object( self, abrs_path):
        locations_file = open(abrs_path, "r")
        locations = {}
        all_abbrs = ""
        for line in locations_file.readlines():
            line_slpitted1 = line.split(":")
            base_location = line_slpitted1[0].lower()
            line_slpitted2 = line_slpitted1[1].split(",")
            locations[base_location] =  list(map(lambda x: x.lower(), line_slpitted2))
            for abr in line_slpitted2:
                all_abbrs+=","+abr.lower()

        return locations

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

    


    def extract_users(self, dataframe):

        start = len(self.users)
        
        
        for index, row in dataframe.iterrows():


            username = row["user_screen_name"]
            userid = row["user_id"]
            userloc = row["user_location"]

            
            if 0 == self.r.exists(str(userid)):
                to_store = json.dumps({"id": str(userid), "screen_name": username, "location": userloc})
                self.r.set(str(userid),to_store)
                if userid not in self.users:
                    self.users.append(userid)
                    self.usernames.append(username)
                    self.locations.append(userloc)
            elif userid not in self.users:
                self.users.append(userid)
                self.usernames.append(username)
                self.locations.append(userloc)

        return (start, len(self.users))


    def try_new_locations(self, start, end, abrs):
        for i in range(start, end):

            if self.kill_now:
                break

            self.locations[i] = self.locations[i].lower()

            user_id = self.users[i]
            user_loc = self.locations[i]

            did_detect = False

            # iterate over all locaitons
            for base_loc in list(abrs.keys()):

                # if user location in abr base. ex. user loc: Los Angl, abr base: Los Angles or
                # it is explicitly in one of abrs list
                if self.locations[i] in abrs[base_loc] or self.locations[i] in base_loc:                        
                    # to_deser = self.r.get(str(user_id))
                    # existing_entry = json.loads(to_deser)
                    # existing_entry["location"] = base_loc
                    # r.set(str(user), json.dumps(existing_entry))
                    
                    self.locations[i] = base_loc

                    # update locations in the database
                    self.detected_users+=1
                    did_detect = True
                    self.updated_indexes.append(i)
                    break
                
                #print(f"baseloc: {base_loc}, los: {locations[base_loc]}, userloc: {user_stats.loc[user,'location']}")

                for abr_temp in abrs[base_loc]:
                    if user_loc in abr_temp:
                        # to_deser = r.get(str(user))
                        # existing_entry = json.loads(to_deser)
                        # existing_entry["location"] = base_loc
                        # r.set(str(user), json.dumps(existing_entry))
                        # update locations in the database

                        self.locations[i]=base_loc
                        self.detected_users+=1
                        did_detect = True
                        self.updated_indexes.append(i)

                        break
                            
                if did_detect:
                    break


            if not did_detect:
                self.undetected_users+=1



            stats = {
                "total_users": self.detected_users + self.undetected_users,
                "detected_users": self.detected_users,
                "detection_rate": self.detection_rate 
                
            }
            #print(stats)

            
            self.comm_queue.put(stats)



        self.detection_rate = self.detected_users / (self.detected_users + self.undetected_users)
        
    def detect_locations(self, file_list, abrs):


        # generated all base locations
        # now we should parse tweets to see if we can find locations of users

        file_index = 0

        for file_path in file_list:

            file_index+=1

            # do detection
            # set stats

            # list(user_stats.keys())
            # iterate over all users


            # first generate set of users from the file
            preprocess_file = pd.read_csv(file_path, lineterminator="\n")
            (start_index, end_index) =  self.extract_users(preprocess_file)

            #get current state to compare
            temp_detected_users = self.detected_users
            temp_undetected_users = self.undetected_users
            total_users = self.detected_users + self.undetected_users


            print(f"Start index {start_index}, end index: {end_index}")

            # now see if new locaiton can be generated
            self.try_new_locations(start_index, end_index, abrs)
            
            print(f" {(temp_detected_users + self.undetected_users) - total_users} users are added. {self.detected_users - temp_detected_users} locations are detected. \n")
            print(f"Total detection rate: {self.detection_rate}\n\n\n")

            stats = {
                "total_users": self.detected_users + self.undetected_users,
                "detected_users": self.detected_users,
                "detection_rate": self.detection_rate ,
                "progress:": f"{file_index}/{len(file_list)}"
            }


            self.comm_queue.put(stats)
        






        stats = {
                "total_users": self.detected_users + self.undetected_users,
                "detected_users": self.detected_users,
                "detection_rate": self.detection_rate,
                "updated_indexes": self.updated_indexes,
                "break": True
        }


        self.comm_queue.put(stats)

        

        user_loc_dict = {
            "userid": self.users,
            "userloc": self.locations,
            "username": self.usernames
        }

        
        to_save_dataframe = pd.DataFrame(user_loc_dict)
        print(to_save_dataframe)


        to_save_dataframe.to_csv("Updated_Locations.csv" )

        data =  [list(to_save_dataframe.iloc[i,:] )for i in self.updated_indexes]
        df = pd.DataFrame(data)
        df.to_csv("only_detected.csv")



       

        

            

    def __init__(self, abrs_path, csv_path,  comm_queue):
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        signal.signal(signal.SIGINT, self.exit_gracefully)

        self.comm_queue = comm_queue

        to_be_preprocessed_files =  self.get_to_be_processed_files(csv_path)
        abrs_obj = self.generate_abrs_object(abrs_path)

        self.detect_locations(abrs= abrs_obj, file_list= to_be_preprocessed_files)




    









