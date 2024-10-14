import tweepy
from kafka import KafkaProducer
import json

api_key = 'your_api_key'
api_secret_key = 'your_api_secret_key'
access_token = 'your_access_token'
access_token_secret = 'your_access_token_secret'

auth = tweepy.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

class TwitterStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        tweet = {
            'text': status.text,
            'user': status.user.screen_name,
            'timestamp': str(status.created_at)
        }
        print(f"Sending tweet: {tweet}")
        producer.send('twitter_topic', tweet)

    def on_error(self, status_code):
        if status_code == 420:
            return False

stream_listener = TwitterStreamListener()
stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
stream.filter(track=['YourBrandName'], languages=['en'])
