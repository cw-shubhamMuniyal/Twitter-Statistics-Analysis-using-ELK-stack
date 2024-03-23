import json
import tweepy  # To consume Twitter's API
import math
import signal
import psutil
#psutil.process(pid=11068).send_signal(signal.SIGINT)


from tweepy.streaming import StreamListener
from tweepy import Stream
from textblob import TextBlob  # predict the sentiment of Tweet, see 'https://textblob.readthedocs.io/en/dev/'
from elasticsearch import Elasticsearch  # pip install Elasticsearch if not intalled yet
from datetime import datetime
from http.client import IncompleteRead

from keys import *

#creating the object of elasticsearch
es = Elasticsearch()

neutral = 0
positive = 0
negative = 0
sentiment_no = 0
counts = 2
class TweetStreamListener(StreamListener):

    # re-write the on_data function in the TweetStreamListener
    # The functiion activate if some data is collected from twitter api
    def on_data(self, data):

        # this function converts json data into python string
        """"
        ob = open("reads.csv", "w")
        ob.write(json.dumps(data))"""

        py_data = json.loads(data)
        # pass Tweet into TextBlob to predict the sentiment
        tweet = TextBlob(py_data["text"]) if "text" in py_data.keys() else None
        global neutral, positive, negative, sentiment_no
        # if the object contains Tweet
        if tweet:
            # determine if sentiment is positive, negative, or neutral
            if tweet.sentiment.polarity < 0:
                sentiment = "negative"
                negative = negative+1
            elif tweet.sentiment.polarity == 0:
                sentiment = "neutral"
                neutral=neutral+1
            else:
                sentiment = "positive"
                positive = positive + 1
            sentiment_no = sentiment_no+1

            # other information along with text and the sentiment
            print("created at=",py_data["created_at"], ",tweet=",py_data["text"],",Screen name=",py_data["user"]["screen_name"],",sentiment=",sentiment, ",polarity=",tweet.sentiment.polarity,"\n")

            print("percentage of sentiment=\n")

            print("positive tweets= %s" % (positive * 100 / sentiment_no), "%")
            sentimentp = math.floor(positive * 100 / sentiment_no)

            print("neutral tweets= %s" % (neutral * 100 / sentiment_no), "%")
            sentimentn = math.floor(neutral * 100 / sentiment_no)

            print("negative tweets= %s" % (negative * 100 / sentiment_no), "%")
            sentimentne = math.floor(negative * 100 / sentiment_no)
            print("number of tweets recieved till are", sentiment_no)

            # extract the first hashtag from the object
            # transform the Hashtags into proper case
            if len(py_data["entities"]["hashtags"]) > 0:
                hashtags = py_data["entities"]["hashtags"][0]["text"].title()
            else:
                # elasticsearch does not take None object
                hashtags = "None"

            # adding information in elasticsearch
            es.index(index="logstash-twitter",
                     # create/inject data into the cluster with index as 'logstash-twitter'
                     # create the naming pattern in Management/Kibana later in order to push the data to a dashboard
                     doc_type="test-type",
                     body={"author": py_data["user"]["screen_name"],
                           "followers": py_data["user"]["followers_count"],
                           # parse the milliscond since epoch to elasticsearch and reformat into datatime stamp in Kibana later
                           "date": datetime.strptime(py_data["created_at"], '%a %b %d %H:%M:%S %z %Y'),
                           "message": py_data["text"] if "text" in py_data.keys() else " ",
                           "hashtags": hashtags,
                           "polarity": tweet.sentiment.polarity,
                           "subjectivity": tweet.sentiment.subjectivity,
                           "count": sentiment_no,
                           "sentiment_finalp": sentimentp,
                           "sentiment_finaln": sentimentn,
                           "sentiment_finalne": sentimentne,
                           "sentiment": sentiment})
        return True

    # on failure, print the error code and do not disconnect
    def on_error(self, status):
        print(status)


if __name__ == '__main__':
    # create instance of the tweepy tweet stream listener
    listener = TweetStreamListener()
    # set twitter keys/tokens
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    searchTerm = input("Enter the search term:\n")

    # The most exception break up the kernel in my test is ImcompleteRead. This exception handler ensures
    # the stream to resume when breaking up by ImcompleteRead
    while True :

        try:
            # create instance of the tweepy stream
            # it creates session and establishes the connection with the respective twitter api
            stream = Stream(auth, listener)
            # search for any valid keyword from twitter
            stream.filter(track=[searchTerm])



        except IncompleteRead:
                continue

        except KeyboardInterrupt:
            # or however you want to exit this loop
            stream.disconnect()
            break