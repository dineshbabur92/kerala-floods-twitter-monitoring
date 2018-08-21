
# coding: utf-8

# In[22]:

# !pip install elasticsearch


# In[7]:

from elasticsearch import Elasticsearch

es = Elasticsearch("http://localhost:9200", http_auth=("", ""))


# In[8]:

INDEX_NAME = 'kerala_floods_tweets_stream_refined'

# es.indices.delete(index=INDEX_NAME)
# es.indices.create(index=INDEX_NAME)
# mapping = {
#     "timestamp" : {"type" : "date"},
#     "full_text": {
#         "type": "text",
#         "fields": {
#             "english": { 
#               "type":     "text",
#               "analyzer": "english",
#                 "fielddata": "true"
#             },
#             "keyword": {
#                 "type": "keyword"
#             }
#         }
#     },
#     "mentions": {
#         "type": "text",
#         "fields": {
#             "keyword": {
#                 "type": "keyword",
#                 "ignore_above": 256
#             }
#         }
#     },
#     "location": {
#         "type": "text",
#         "fields": {
#             "keyword": {
#                 "type": "keyword",
#                 "ignore_above": 256
#             }
#         }
#     },
#     "screen_name": {
#         "type": "text",
#         "fields": {
#             "keyword": {
#                 "type": "keyword",
#                 "ignore_above": 256
#             }
#         }
#     },
#     "name": {
#         "type": "text",
#         "fields": {
#             "keyword": {
#                 "type": "keyword",
#                 "ignore_above": 256
#             }
#         }
#     },
#     "hashtags": {
#         "type": "text",
#         "fields": {
#             "keyword": {
#                 "type": "keyword",
#                 "ignore_above": 256
#             }
#         }
#     },
#     "retweeted": {
#         "type": "boolean"
#     },
#     "is_retweeted_status": {
#         "type": "boolean"
#     },
#     "retweeted_screen_name":{
#         "type": "text",
#         "fields": {
#             "keyword": {
#                 "type": "keyword",
#                 "ignore_above": 256
#             }
#         }
#     },
#     "retweeted_name":{
#         "type": "text",
#         "fields": {
#             "keyword": {
#                 "type": "keyword",
#                 "ignore_above": 256
#             }
#         }
#     },
#     "retweeted_user_followers":{
#         "type": "long"
#     },
#     "created_at": {
#         "type": "date",
#         "format": "EEE MMM dd HH:mm:ss Z yyyy"
#     },
#     "is_geo_enabled": {
#         "type": "boolean"
#     },
#     "coordinates": {
#       "type": "geo_point"
#     }
# }

# es.indices.put_mapping("tweets", {'properties':mapping}, [INDEX_NAME])


# In[13]:

import tweepy
import json

consumer_key = ""
consumer_secret = ""
access_token = ""
access_token_secret = ""

class StdOutListener(tweepy.StreamListener):
    
    def on_data(self, data):
        global i        
        doc = json.loads(data)
        
        try:   
            tweet = (doc["extended_tweet"]["full_tweet"] 
                if "extended_tweet" in doc.keys() 
                else doc["retweeted_status"]["extended_tweet"]["full_text"])
        except:
            tweet = doc["text"]
            
        mentions_array = []
        for user_mention in doc['entities']["user_mentions"]:
            mentions_array.append(user_mention["screen_name"])
            
        hashtags_array = []
        for hashtag in doc['entities']["hashtags"]:
            hashtags_array.append(hashtag["text"])
            
        es.index(
            index=INDEX_NAME,
            doc_type = "tweets",
            body = {
                "timestamp": doc["timestamp_ms"],
                "full_text": tweet,
                "mentions": mentions_array,
                "location": doc["user"]["location"],
                "name": doc["user"]["name"],
                "screen_name": doc["user"]['screen_name'],
                "hashtags": hashtags_array,
                "retweeted": doc["retweeted"],
                "is_retweeted_status": True if "retweeted_status" in doc.keys() else False,
                "retweeted_screen_name": doc["retweeted_status"]["user"]["screen_name"] if "retweeted_status" in doc.keys() else None,
                "retweeted_name": doc["retweeted_status"]["user"]["name"] if "retweeted_status" in doc.keys() else None,
                "retweeted_user_followers": doc["retweeted_status"]["user"]["followers_count"] if "retweeted_status" in doc.keys() else None,
                "created_at": doc["created_at"],
                "is_geo_enabled": doc["user"]["geo_enabled"],
                "coordinates": doc["coordinates"]
            }
        )
#         print(doc if doc["user"]["geo_enabled"] == True else None)
    def on_error(self, status_code):
        print('Got an error with status code: ' + str(status_code))
        return True # To continue listening
    def on_timeout(self):
        print('Timeout...')
        return True # To continue listening
if __name__ == '__main__':
    l = StdOutListener()
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    print("Indexing all new tweets for #:")
    
    i=0
    def start_stream(auth,l):
        while True:
            try:
                stream = tweepy.Stream(auth, l)
                stream.filter(track=[
                    # common
                    # "kerala",
                    "kerala flood",
                    "kerala sos",
                    "kerala rain",
                    "kerala rescue",
                    "kerala relief",
                    "kerala help",
                    "kerala donation",
                    "keralafloods",
                    "keralaflood",
                    "keralafloodsrelief",
                    "keralafloodsrescue",
                    "keralafloods2018",
                    "keralafloodrelief",
                    "keralafloodrescue",
                    "keralaflood2018",
                    "keralasos",
                    "keraladonation",
                    "keraladonationchallenge",

                    "keralarain",
                    "keralarains",
                    "keralarescue",
                    "keralrelief",

                    "cmokerala",
                    "prayforkerala", 
                    "pray for kerala",
                    "savekerala",
                    "save kerala",
                    "opmadad",
                    
                    # not needed anyway, kerala is there as a keyword
                    # diseases
                    "kerala typhoid",
                    "kerala cholera",
                    "kerala chikungunya",
                    "kerala leptospirosis",
                    "kerala hepatitis A",
                    "kerala malaria",
                    "kerala dengue",
                    "kerala yellow fever",
                    "kerala west nile Fever",
                    "kerala gastrointestinal infection",
                    "kerala tuberculosis",
                    "kerala hypothermia",
                    
                    # symptoms
                    "kerala chills",
                    "kerala cold",
                    "kerala cough",
                    "kerala fever",
                    "kerala headache",
                    "kerala muscle pain",
                    "kerala nausea",
                    "kerala vomiting",
                    "kerala diarrhea",
                    "kerala rashes",
                    "kerala infection",
                    "kerala dermatitis",
                    "kerala conjunctivitis",
                    "kerala fatigue",
                    "kerala rash",
                    "kerala sore throat",
                    "kerala hay fever"
                ]) #Enter keyword(s)
            except Exception as e:
                print("error ", e)
                continue
    start_stream(auth,l)



# In[ ]:



