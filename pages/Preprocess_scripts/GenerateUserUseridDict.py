master_folder = ""
import os
import pandas as pd

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


    

files = get_to_be_processed_files(master_folder)


users = []
usernames = []

def extract_users( file_path):


                # first generate set of users from the file
    dataframe = pd.read_csv(file_path, lineterminator="\n")

    start = len( users)
    
    
    for index, row in dataframe.iterrows():


        username = row["user_screen_name"]
        userid = row["user_id"]
        userloc = row["user_location"]

        
      
        if userid not in  users:
            users.append(userid)
            usernames.append(username)
       

    return (start, len( users))



for file in files:
    extract_users(file)



    user_loc_dict = {
        "userid": users,
        "username": usernames
    }

    
    to_save_dataframe = pd.DataFrame(user_loc_dict)
    print(to_save_dataframe)


    to_save_dataframe.to_csv("user-id_dict.csv" )
