from kafka import KafkaConsumer
from textblob import TextBlob
import json

def analyze_sentiment(text):
    analysis = TextBlob(text)
    return analysis.sentiment.polarity

def json_deserializer(data):
    return json.loads(data.decode('utf-8'))

consumer = KafkaConsumer(
    'twitter_topic',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=json_deserializer,
    auto_offset_reset='earliest',
    enable_auto_commit=True
)

if __name__ == "__main__":
    print("Listening for tweets...")
    for message in consumer:
        tweet = message.value
        sentiment = analyze_sentiment(tweet['text'])
        print(f"Tweet: {tweet['text']}")
        print(f"Sentiment: {sentiment}")

        if sentiment < -0.1:
            print(f"Alert! Negative tweet detected: {tweet['text']} from {tweet['user']}")
