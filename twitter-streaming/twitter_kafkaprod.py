from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import SimpleProducer, KafkaClient
import sys

access_token = "your_own"
access_token_secret =  "your_own"
consumer_key =  "your_own"
consumer_secret =  "your_own"

class StdOutListener(StreamListener):
    def on_data(self, data):
        producer.send_messages("twitterstream", data.encode('utf-8'))
        print (data)
        return True
    def on_error(self, status):
        print (status)

words=sys.argv[1]
kafka = KafkaClient("localhost:29092")
producer = SimpleProducer(kafka)
l = StdOutListener()
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
stream = Stream(auth, l)
stream.filter(track=words.split(" "))

