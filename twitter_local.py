#!/usr/bin/env python
# -*- coding: utf-8 -*-
#

import json
import os
import queue
import threading
import time
from datetime import datetime

import boto3
import tweepy

# AWS and Twitter API keys
bearer_token = '**********************'
bucket = "twitter-bucket"
keyword = "#nba"
client_id = "**********************"
aws_key_id = '**********************'
aws_secret = '**********************'

total_tweets_fetched = 0


def consumer():
    tweet_list = []
    total_count = 0
    s3_client = boto3.client('s3', region_name='us-west-2')

    while not event.is_set() or not pipeline.empty():
        try:
            data = pipeline.get()
            data_dict = json.loads(data)
            data_dict.update({"keyword": keyword})
            tweet_list.append(json.dumps(data_dict))
            total_count += 1

            if total_count % 2 == 0:
                print(".", end="")
            if total_count % 5 == 0:
                print(f"\n{total_count} tweets retrieved.")
                filename = f"tweets_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
                print(f"==> Writing {len(tweet_list)} records to {filename}")
                with open(filename, 'w') as tweet_file:
                    tweet_file.writelines(tweet_list)

                # Upload to S3
                now = datetime.now()
                key = f"{keyword}/{now.year}/{now.month}/{now.day}/{now.hour}/{now.minute}/{filename}"
                print(f"==> Uploading to {key}")
                s3_client.upload_file(filename, bucket, key)
                print(f"==> Uploaded to {key}")
                tweet_list = []
                os.remove(filename)
        except Exception as e:
            print(f"Error in consumer thread: {e}")


class JSONStreamProducer:
    def __init__(self, client, keyword):
        self.client = client
        self.keyword = keyword

    def fetch_and_enqueue(self):
        global total_tweets_fetched  # Use the global counter
        try:
            # Fetch tweets matching the keyword
            response = self.client.search_recent_tweets(query=self.keyword, tweet_fields=["created_at", "text"], max_results=10)
            if response.data:
                for tweet in response.data:
                    pipeline.put(json.dumps({"id": tweet.id, "text": tweet.text, "created_at": tweet.created_at.isoformat()}))
                    total_tweets_fetched += 1  # Increment the total counter
                    
                    # Stop fetching after 10 tweets
                    if total_tweets_fetched >= 10:
                        print("Fetched 10 tweets. Stopping...")
                        event.set()
                        break  # Exit the loop to stop fetching
        except tweepy.errors.TooManyRequests as e:
            print("Rate limit hit. Exiting program...")
            event.set()
        except Exception as e:
            print(f"Error fetching tweets: {e}")


if __name__ == "__main__":

    client = tweepy.Client(
        "**********************"
    )
    
    pipeline = queue.Queue()
    event = threading.Event()

    producer = JSONStreamProducer(client, keyword)

    # Start consumer thread
    consumer_thread = threading.Thread(target=consumer)
    consumer_thread.start()


    # Start Twitter streaming
    try:
        # Periodically fetch tweets and enqueue them
        while not event.is_set():
            producer.fetch_and_enqueue()
            time.sleep(30)  # Fetch tweets every 30 seconds
    except KeyboardInterrupt:
        print("Stopping stream...")
        event.set()

    # Wait for consumer thread to finish
    consumer_thread.join()
    print("Streaming stopped.")
