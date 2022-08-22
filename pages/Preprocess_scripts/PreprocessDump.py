import signal
import time
import os
import sys
import json
from datetime import datetime
import signal
import pandas as pd
from multiprocessing import Queue


class PreprocessDump:


    stat_file = "stats"


    def clear_dictionary(self, tweets):
        for key1 in tweets:
            for key2 in tweets[key1]:
                tweets[key1][key2] = None

    def max_mem_reached(self, dicto, max_size):

        total_size = 0
        for key1 in dicto:
            for key2 in dicto[key1]:
                total_size += sys.getsizeof(dicto[key1][key2])
        # mb to bytes
        if total_size > max_size * 1000000:
            return True
        
        return False
        
        

    

    

    def isabout(self, keywords, text):
        for k in keywords:
            if k in text:
                return True
            l = text.lower()
            if k in l:
                return True
        return False


    def what_isit_about(self, json_obj, text):
        relevant_topics = []
        index = 0
        for topic in json_obj:
            if self.isabout(keywords=topic["keywords"], text = text):
                relevant_topics.append(index)
            index+=1
        return relevant_topics




    def preprocess_text(self, text):
        text = text.replace("\n", ' ').replace("\r", " ").replace("\t", " ").replace("  "," ").strip()
        return text

    def remove_new_lines(self, input):
        return str(input).replace("\n", " ")



    def is_dict_empty(self, tweets):
        
        for key1 in tweets:
            for key2 in tweets[key1]:
                try:
                    if not tweets[key1][key2].empty:
                        return False
                except Exception as e:
                    print(e)
                    # this error means that entry has been set to None
                    # no problem throwing exception here
                    pass
        return True

    def save_dictionary(self, tweets, output_folder):
        for key1 in tweets:
            for key2 in tweets[key1]:
                out_file = os.path.join(output_folder)
                if not os.path.exists(out_file):
                    os.makedirs(out_file)

                out_file = os.path.join(output_folder, key1)
                if not os.path.exists(out_file):
                    os.makedirs(out_file)

                out_file = os.path.join(output_folder, key1, key2)
                if not os.path.exists(out_file):
                    os.makedirs(out_file)


                current_time = datetime.now()
                out_file = os.path.join(output_folder, key1, key2, str(current_time))
                
                print(out_file)
                try:
                    tweets[key1][key2].to_csv(out_file+ ".csv", index = False)
                except Exception as e:
                    print(e)
                    # this means this entry has no been initialize in this batch
                    pass

        self.clear_dictionary(tweets)


    def clear_dictionary(self, tweets):
        for key1 in tweets:
            for key2 in tweets[key1]:
                tweets[key1][key2] = None

    def max_mem_reached(self, dicto, max_size):

        total_size = 0
        for key1 in dicto:
            for key2 in dicto[key1]:
                total_size += sys.getsizeof(dicto[key1][key2])
        # mb to bytes
        if total_size > max_size * 1000000:
            return True
        
        return False
        
        

        
    def save_dictionary(self ,tweets, output_folder):
        for key1 in tweets:
            for key2 in tweets[key1]:
                out_file = os.path.join(output_folder)
                if not os.path.exists(out_file):
                    os.makedirs(out_file)

                out_file = os.path.join(output_folder, key1)
                if not os.path.exists(out_file):
                    os.makedirs(out_file)

                out_file = os.path.join(output_folder, key1, key2)
                if not os.path.exists(out_file):
                    os.makedirs(out_file)


                current_time = datetime.now()
                out_file = os.path.join(output_folder, key1, key2, str(current_time))
                
                try:
                    tweets[key1][key2].to_csv(out_file+ ".csv", index = False)
                except Exception as e:
                    print(e)
                    # this means this entry has no been initialize in this batch
                    pass

        self.clear_dictionary(tweets)


    def preprocess_file(self, topics_json ,file_path, output_folder, max_size):
        # need this parameter
    
        with open(file_path, "r") as openfileobject:
            # now we should create lists for each language and each topic
            

            # this dictionary will be used to translate topic index to 
            # its corresponding buffer
            
            # access will be: [<topic>][<lang>] -> buffer


            topic_to_buffer = {}
            total_tweet_counter = 0
            
            revealed_topics = set()
            languages = set()

            
        
            for line in openfileobject:
                try:
                    if not self.stats_query_queue.empty():
                        query = self.stats_query_queue.get(block=False)
                        self.stats_response_queue.put(f"You sent {total_tweet_counter}", block=False)
                except Exception as e :
                    file = open("Sa", "w")
                    file.write(str(e))
                    file.close()

                # change here
                if self.kill_now:
                    self.save_dictionary(topic_to_buffer, output_folder)
                    return

                total_tweet_counter+=1
                

                    
                # create json object of that line
                tweet_json = json.loads(line)

                
                #create new tweet object with necessary fields
                temp_dict = self.preprocess_downloaded(tweet_json)
                temp_dict.update({ 
                    "user_description": self.remove_new_lines(tweet_json["user"]["description"]), 
                    "user_created_at": self.remove_new_lines(tweet_json["user"]["created_at"]), 
                    "user_followers_count": self.remove_new_lines(tweet_json["user"]["followers_count"]), 
                    "verified": self.remove_new_lines(tweet_json["user"]["verified"]), 
                    "user_location": self.remove_new_lines(tweet_json["user"]["location"]), 
                    "user_id": self.remove_new_lines(tweet_json["user"]["id"]), 
                    "user_screen_name": self.remove_new_lines(tweet_json["user"]["screen_name"])
                })

                
                # if lang of tweet and topic is not same we should break?
                # decide which dump to insert
                relevant_topics = self.what_isit_about(topics_json, tweet_json["text"])
                if len(relevant_topics) == 0 or tweet_json["lang"] not in languages:
                    # then we will save this tweet to other
                    if "other" not in topic_to_buffer:
                        topic_to_buffer["other"] = {}
                    if tweet_json["lang"] not in topic_to_buffer["other"]:
                        topic_to_buffer["other"][tweet_json["lang"]] = pd.DataFrame(columns=["created_at", "in_reply_to_user_id", "referenced_tweet", "ref_type", "text", "id", "source", "user_description", "user_created_at", "user_followers_count", "verified", "user_location", "user_id", "user_screen_name"])
                        topic_to_buffer["other"][tweet_json["lang"]].append(temp_dict, ignore_index=True)


                for topic_index in relevant_topics:
                    
                    if topics_json[topic_index]["name"] not in topic_to_buffer:
                        topic_to_buffer[topics_json[topic_index]["name"]] = {}

                    if  tweet_json["lang"] not in topic_to_buffer[topics_json[topic_index]["name"]]:
                        topic_to_buffer[topics_json[topic_index]["name"]][tweet_json["lang"]] = pd.DataFrame(columns=["created_at", "in_reply_to_user_id", "referenced_tweet", "ref_type", "text", "id", "source", "user_description", "user_created_at", "user_followers_count", "verified", "user_location", "user_id", "user_screen_name"])
                        print(f"New topic and lang revealed: {topics_json[topic_index]['name']} , {tweet_json['lang']} ")
                    try:
                        if None == topic_to_buffer[topics_json[topic_index]["name"]][tweet_json["lang"]]    :
                            topic_to_buffer[topics_json[topic_index]["name"]][tweet_json["lang"]] = pd.DataFrame(columns=["created_at", "in_reply_to_user_id", "referenced_tweet", "ref_type", "text", "id", "source", "user_description", "user_created_at", "user_followers_count", "verified", "user_location", "user_id", "user_screen_name"])
                    except Exception as e:
                         #this means this entry is already a dataframe
                        pass
                        

                    topic_to_buffer[topics_json[topic_index]["name"]][tweet_json["lang"]] = topic_to_buffer[topics_json[topic_index]["name"]][tweet_json["lang"]].append(temp_dict, ignore_index=True)
                    
                    # st.write(topic_to_buffer[topics_json[topic_index]["name"]][topics_json[topic_index]["lang"]])

                # we should not overflow memory so check if more memory available
                should_quit = self.max_mem_reached(topic_to_buffer, max_size)
                if should_quit:
                    #st.write("One bathch is on its way to save")
                    #then save and empty our dictionary
                    self.save_dictionary(topic_to_buffer, output_folder)


            if not self.is_dict_empty(topic_to_buffer):
                self.save_dictionary(topic_to_buffer, output_folder)



    def preprocess_downloaded(self, tweet):

        tweet_dict = {}
        tweet_dict["created_at"] = self.remove_new_lines(tweet["created_at"])
        tweet_dict["in_reply_to_user_id"] = self.remove_new_lines(tweet["in_reply_to_user_id"])

        try:
            #twitter api v2 retweet or quote distinction
            #this will throw error 
            tweet_dict["referenced_tweet"] = self.remove_new_lines(tweet["referenced_tweets"].split("id=")[-1].split[" "][0])
            #extract id from ref entry

            if "replied_to" in str(tweet["referenced_tweets"][-1]):
                tweet_dict["ref_type"] = "quote"
            if "retweeted" in str(tweet["referenced_tweets"][-1]):
                tweet_dict["ref_type"] = "retweet"

        except:
            # twitter api v1 retweeted_status field is used to distinguish a retweet
            try:
                
                # this will throw exception if retweeted_status does not occur in response
                tweet_dict["referenced_tweet"] = "{0}".format(tweet["retweeted_status"]["id"])
                tweet_dict["ref_type"] = "retweet"

            except: 
                try: 
                    # this will throw exception if quoted_status does not occur in response
                    tweet_dict["referenced_tweet"] = "{0}".format(tweet["quoted_status"]["id"])
                    tweet_dict["ref_type"] = "quote"

                except: 
                    # this part is for the format that is downloaded with this tool
                    try:
                        tweet_dict["referenced_tweet"] = tweet_dict["referenced_tweet"]
                        tweet_dict["ref_type"] = tweet_dict["ref_type"]
                    except:
                        # no matching format so returning None for references
                        tweet_dict["referenced_tweet"] = None
                        tweet_dict["ref_type"] = None




        tweet_dict["text"] = self.preprocess_text(tweet["text"])
        tweet_dict["id"] = self.remove_new_lines(tweet["id"])
        tweet_dict["source"] = self.remove_new_lines(tweet["source"])


        return tweet_dict


    def preprocess_general_dump(self, json_obj, dump_file_path, max_mem):

        output_folder = "tweets_dump_csv"

        # First we create directory if that folder does not exists or is not a folder
        if not os.path.exists(output_folder) and not os.path.isdir(output_folder):
            os.makedirs(output_folder)

        
        output_folder = os.path.join("tweets_dump_csv" , str(datetime.now()))
        if not os.path.exists(output_folder) and not os.path.isdir(output_folder):
            os.makedirs(output_folder)


        languages = set()
        topics = set()
        for topic in json_obj:
            languages.add(topic["lang"])
            topics.add(topic["name"])
        

        # Now we should preprocess files recursively
        current_folder = dump_file_path
        to_be_processed_file_list = os.listdir(current_folder)
        templist = []
        for i in range(len(to_be_processed_file_list)):
            templist.append(os.path.join(current_folder , to_be_processed_file_list[i]))
        to_be_processed_file_list = templist
        
        index = 0



        
        
        
        while True:
            if len(to_be_processed_file_list) <= index:
                break

            isDir = os.path.isdir(to_be_processed_file_list[index])
            if isDir: 
                new_file_list = os.listdir(to_be_processed_file_list[index])
                to_be_appended = list(map(lambda x  : os.path.join(to_be_processed_file_list[index], x), new_file_list))
                to_be_processed_file_list.extend(to_be_appended)
                to_be_processed_file_list.remove(to_be_processed_file_list[index])


            else: 
                self.preprocess_file(json_obj, to_be_processed_file_list[index], output_folder, max_mem)
                index+=1
        

        


    def exit_gracefully(self, *args):
        self.kill_now = True


    kill_now = False
    def __init__(self, json_file_path, dump_file_path, max_mem, stats_query_queue, stats_response_queue):
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        signal.signal(signal.SIGINT, self.exit_gracefully)

        json_file = open(json_file_path, "r")
        json_obj = json.load(json_file)

        self.stats_query_queue = stats_query_queue
        self.stats_response_queue = stats_response_queue

        self.preprocess_general_dump(json_obj=json_obj, dump_file_path=dump_file_path, max_mem=max_mem)
        
        print("Exiting")


#   PreprocessDump(json_file_path=sys.argv[1], dump_file_path=sys.argv[2],  max_mem=int(sys.argv[3]))