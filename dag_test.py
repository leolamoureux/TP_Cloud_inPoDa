import random
import re
import statistics
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from textblob import TextBlob



def extract_hashtag(tweet):
    if "#" in tweet:
        return re.findall(r"#(\w+)", tweet)
    else:
        return ["any"]
    
def topic_identification(tweet):
    topics = ["sport", "politique", "sciences"]
    return random.choice(topics)

def sentiment_analysis(tweet):
    blob = TextBlob(tweet)
    return statistics.mean([s.sentiment.polarity for s in blob.sentences])

def call_topic_identification(tweets, results, **kwargs):
    for t in tweets:
        if not t["id"] in results:
            results[t["id"]] = {} 
        results[t["id"]]["topic"] = topic_identification(t["text"])
    return results

def call_hashtag_extraction(tweets, results, **kwargs):
    for t in tweets:
        if not t["id"] in results:
            results[t["id"]] = {} 
        results[t["id"]]["hashtags"] = list(extract_hashtag(t["text"]))
    return results

def call_sentiment_analysis(tweets, results, **kwargs):
    for t in tweets:
        if not t["id"] in results:
            results[t["id"]] = {} 
        results[t["id"]]["sentiment"] = sentiment_analysis(t["text"])
    return results

def show_result(results, **kwargs):
    print(results)

    return results

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10),
}

dag = DAG(
    'InPoDa_3_test',
    default_args=default_args,
    description='My WSDL calling DAG',
    schedule_interval=timedelta(days=1),
)
tweet_1 = {
        "_id": "1421616335700824064",
        "public_metrics": {
            "retweet_count": 0,
            "reply_count": 0,
            "like_count": 1,
            "quote_count": 0
                           },
        "id": "1421616335700824064",
        "conversation_id": "1421616335700824064",
        "author_id": "1339914264522461187",
        "text": "Goumin des éléphants joueurs la même fatigue même 😫 #twitter225",
        "geo": {
            "place_id": "00b8943291443c8c"
                },
        "lang": "fr",
        "created_at": "2021-07-31T23:38:41.000Z",
        "entities": {
        "hashtags": [
                    {
            "start": 52,
            "end": 63,
            "tag": "twitter225"
                    }
                    ]
                }
            }

tweet_2 = {
        "_id": "1421616335700824064",
        "public_metrics": {
            "retweet_count": 0,
            "reply_count": 0,
            "like_count": 1,
            "quote_count": 0
                           },
        "id": "1421616335700824064",
        "conversation_id": "1421616335700824064",
        "author_id": "1339914264522461187",
        "text": "Goumin des éléphants joueurs la même fatigue même 😫 #twitter225",
        "geo": {
            "place_id": "00b8943291443c8c"
                },
        "lang": "fr",
        "created_at": "2021-07-31T23:38:41.000Z",
        "entities": {
        "hashtags": [
                    {
            "start": 52,
            "end": 63,
            "tag": "twitter225"
                    }
                    ]
                }
            }
tweets = [tweet_1, tweet_2]
print(f"{len(tweets)} tweets.")
results = {}

task_1 = PythonOperator(
    task_id='call_topic_identification',
    python_callable=call_topic_identification,
    op_args=[tweets, results],
    dag=dag,
)

task_2 = PythonOperator(
    task_id='call_hashtag_extraction',
    python_callable=call_hashtag_extraction,
    op_args=[tweets, results],
    dag=dag,
)

task_3 = PythonOperator(
    task_id='call_sentiment_analysis',
    python_callable=call_sentiment_analysis,
    op_args=[tweets, results],
    dag=dag,
)

task_4 = PythonOperator(
    task_id='show_result',
    python_callable=show_result,
    op_args=[results],
    dag=dag,
)


task_1 >> task_2 >> task_3 >> task_4
