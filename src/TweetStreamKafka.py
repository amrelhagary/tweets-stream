import tweepy
import logging
import sys
import configparser 
from tweepy import OAuthHandler
from tweepy import Stream 
from tweepy.streaming import StreamListener
import socket 
import json
import os
from kafka import KafkaProducer
import argparse

# Stream Listener
class StdOutListener(StreamListener):
    def __init__(self, kafkaTopic):
        self.topic = kafkaTopic
    
    def on_data(self, data):
        producer.send(self.topic, data)
        print(data)
        return True

    def on_error(self, status):
        print(status)


try:
    # Load Configuration
    config = configparser.ConfigParser()
    config.read(os.path.join(os.path.dirname(__file__), "config.ini"))

    access_token = config["TWITTER_API"]["AccessToken"]
    access_secret = config["TWITTER_API"]["AccessTokenSecret"]
    consumer_key = config["TWITTER_API"]["API_Key"]
    consumer_secret = config["TWITTER_API"]["API_Secret_Key"]

    # Argument parser
    parser = argparse.ArgumentParser(description="Stream tweets from twitter to kafka")
    parser.add_argument("--twitter-filter", "-f", help="Filter tweets by using keyboards", type= str, default="pyspark")
    parser.add_argument("--kafka-broker", "-k", help="Kafka Bootstrap server", type= str, default= "localhost:9092")
    parser.add_argument("--kafka-topic", "-t", help="Kafka streaming topic", type= str, default= "tweets")

    args = parser.parse_args()
    twitter_filter = args.twitter_filter
    bootstrap_servers = args.kafka_broker
    kafka_topic = args.kafka_topic

    print(twitter_filter, bootstrap_servers, kafka_topic)
    # Create Kafka Producer and Serlizer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers, value_serializer=lambda v: json.dumps(v).encode("utf-8"))

    # Create Stream Listener
    listener = StdOutListener(kafka_topic)

    # Twitter Authentication
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    if not api.verify_credentials():
        raise "Not Authenticated"

    # twitter_stream will get the actual live tweet data
    twitter_stream = Stream(auth = api.auth, listener = listener)
    # filter the tweet feeds related to "corona"
    twitter_stream.filter(track=[twitter_filter])

except IOError as e:
    if e.errno == errno.EPIPE:
        # EPIPE error
        print("EPIPE error : {}".format(e))
except KeyboardInterrupt:
    # Close socket server
    print("Interrupted")
    producer.close()
    sys.exit(0)
except:
    print("Unexpected error:", sys.exc_info()[0])
    raise