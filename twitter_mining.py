'''
This program is used to receive the data from Twitter stream, store the data into MongoDB database, and 
then mine the word frequency and tweet frequency.
'''

import json
import pymongo
import tweepy
import time
import string
from collections import Counter
from nltk.tokenize import TweetTokenizer
from nltk.corpus import stopwords
import matplotlib.pyplot as plt
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd
import numpy as np

# function to tokenize a text: 1. lowercase, 2. tokenize, 3. stopwords removal, 4. digits removal
def process(text, tokenizer=TweetTokenizer(), stopwords=[]):
    text = text.lower()
    tokens = tokenizer.tokenize(text)
    return [word for word in tokens if word not in stopwords and not word.isdigit()]

# define the class for receiving data from Twitter stream
class CustomStreamListener(tweepy.StreamListener):
    # initialize the listener
    def __init__(self, api, time_limit=30): # time in seconds
        self.api = api
        super(tweepy.StreamListener, self).__init__()
        # connect to mongoDB server in the local host
        self.db = pymongo.MongoClient('localhost', 27017).db2 
        # the start time
        self.start_time=time.time()
        # the time period for receiving Twitter streams
        self.limit = time_limit

    # receive data
    def on_data(self, tweet):
        # only receive data for a time priod
        if (time.time() - self.start_time) < self.limit:
            full_data = json.loads(tweet)
            created_time = full_data['created_at']
            user_id = full_data['id_str']
            text = full_data['text']
            document_record = {'time':created_time, 'user_id': user_id, 'text':text}
            print text
            # insert the document into mongoDB
            self.db.tweets.insert_one(document_record)
            return True
        else:
            return False
            
    def on_error(self, status_code):
        print status_code
        return True 

    def on_timeout(self):
        return True 


# ******** Main Program *********************

# parameters for Twitter App (please use your parameters here)
consumer_key = '******************'
consumer_secret = '******************'
access_key = '******************'
access_secret = '******************'

# authentication
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_key, access_secret)
api = tweepy.API(auth)
sapi = tweepy.streaming.Stream(auth, CustomStreamListener(api))
# the list of keywords for filtering tweets
keyword_list = ['Election'] 
sapi.filter(track = keyword_list, languages = ['en'])  
print 'Tweets have been successfully stored into mongoDB.'

# *** retrive data from mongoDB ***
conn =pymongo.MongoClient('localhost', 27017)
print 'Connected successfully to MongoDB!'
# create a database
db_name='db2'
db=conn[db_name]
# collection
colection = db.tweets
# query: find all documents
results = colection.find()
# close the mongoDB connection
conn.close()
# convert the results to a list
list_results=list(results)
# print the time and the text
for record in list_results:
    print 'At %s: \t %s.'% (record['time'],record['text'])

# *** word frequency mining ****
# tokenizer
tweet_tokenizer = TweetTokenizer()
# punctuation list
punct = list(string.punctuation)
# download 127 Englisg stop words
import nltk
nltk.download('stopwords')
# list of stop words and punctuations
stopword_list = stopwords.words('english') + punct + ['rt', 'via']

# record the number of occurences for each word
tf = Counter()
all_dates = []

# get the text and the time
for element in list_results:
    message = element['text']
    tokens = process(text = message, tokenizer = tweet_tokenizer, stopwords = stopword_list)
    all_dates.append(element['time'])
    # update word frequency
    tf.update(tokens)

# convert the counter to a sorted list (tf_sorted is a list of 2-tuples)
tf_list_sorted = sorted(tf.items(), key = lambda pair: pair[1], reverse = True) 
# print each word and its frequency
for item in tf_list_sorted:
    print item[0], item[1]
    
# print the top-30 frequent words and their frequencies
y1 = [x[1] for x in tf_list_sorted[:30]]
x1 = range(1, len(y1) + 1)
fig1 = plt.figure()
plt.bar(x1, y1)
plt.xlabel("Word index")
plt.title("Term Frequencies")
plt.ylabel("Frequency")
fig1.savefig('term_distribution.jpg')

# *** tweet time series ****
ones = np.ones(len(all_dates))
idx = pd.DatetimeIndex(all_dates)
# the actual time series
original_series = pd.Series(ones, index = idx).sort_index()
# time series with step of 10 seconds
revised_series = original_series.resample('10S').sum()

# print the time series 
x2 = [x*10 for x in range(len(revised_series))]
y2 = list(revised_series)
fig2 = plt.figure()
plt.bar(x2, y2)
plt.title("Time series for real-time tweets")
plt.ylabel("Number of tweets")
plt.xlabel("Time [S]")
fig2.savefig('tweet_time_series.jpg')

