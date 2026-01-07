import tweepy
import pandas as pd
from datetime import datetime
import s3fs
import re
from textblob import TextBlob


def clean_text(text):
    text = re.sub(r"http\S+", "", text)
    text = re.sub(r"@\w+", "", text)
    text = re.sub(r"#", "", text)
    return text.strip()

def get_sentiment(text):
    analysis = TextBlob(text)
    if analysis.sentiment.polarity > 0:
        return "Positive"
    elif analysis.sentiment.polarity < 0:
        return "Negative"
    else:
        return "Neutral"

def extract_hashtags(text):
    return re.findall(r"#(\w+)", text)

def run_twitter_etl():

    access_key="2008543127557337091-o8p3zV8Q8a3T9iXz8z8wMRgm0d6tOW"
    access_secret="OWOnuTnrrpGAOCWnZO627YNHI0F5Zjvc1Z0FifQktFEBB"
    consumer_key="99FLHdWg6nfqOVuAiaHIHDhtY"
    consumer_secret="Oi8uf1WpfW0dpMz14JfiWGx5j5HkoWoonNvlvahyJtBKYWd5Mg"

    client = tweepy.Client(bearer_token="AAAAAAAAAAAAAAAAAAAAAC0v6wEAAAAABYDAjGpv5q2EsJyZOvecpWSmJXo%3Dseud02Fix6Do93kyUC1Q8fbgh0dP8DhZOTuiqmuBY7uc3Y1KeU")

    elon_id = "44196397"

    try:
        response = client.get_users_tweets(
            id=elon_id,
            max_results=10,
            tweet_fields=["created_at","public_metrics"]
        )
    except tweepy.TooManyRequests:
        print("Rate limit hit — skipping run")
        return

    if not response.data:
        print("No tweets found")
        return

    rows = []
    hashtags = []
    

    for t in response.data:
        text = clean_text(t.text)

        rows.append({
            "user": "elonmusk",
            "text": text,
            "likes": t.public_metrics["like_count"],
            "retweets": t.public_metrics["retweet_count"],
            "sentiment": get_sentiment(text),
            "created_at": t.created_at
        })

        hashtags.extend(extract_hashtags(t.text))

    df = pd.DataFrame(rows)

    hashtag_df = (
        pd.Series(hashtags)
        .value_counts()
        .reset_index()
        .rename(columns={"index":"hashtag", 0:"count"})
    )

    fs = s3fs.S3FileSystem()

    df.to_csv("s3://airflow-pratik/elonmusk_tweets.csv", index=False, storage_options={"s3fs": fs})
    hashtag_df.to_csv("s3://airflow-pratik/trending_hashtags.csv", index=False, storage_options={"s3fs": fs})

    print("Tweets and hashtags uploaded to S3")


