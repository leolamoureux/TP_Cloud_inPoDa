from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from pymongo import MongoClient
from suds.client import Client

client = MongoClient("mongodb://localhost:27017");
print("Connection Successful")

##### Récupération collection versailles tweets ##########
db = client['Projet_SOA']
tweets_collection = db['versailles_tweets']

wsdl_1 = "http://127.0.0.1:8000/topic_service?wsdl"
wsdl_2 = "http://127.0.0.1:8000/hashtag_service?wsdl"
wsdl_3 = "http://127.0.0.1:8000/sentiment_service?wsdl"

def call_topic_identification(tweets, results, **kwargs):
    client = Client("http://127.0.0.1:8000/topic_service?wsdl")
    for t in tweets:
        if not t["id"] in results:
            results[t["id"]] = {} 
        results[t["id"]]["topic"] = client.service.topic_identification(t["text"])
    return results

def call_hashtag_extraction(tweets, results, **kwargs):
    client = Client("http://127.0.0.1:8000/hashtag_service?wsdl")
    for t in tweets:
        if not t["id"] in results:
            results[t["id"]] = {} 
        results[t["id"]]["hashtags"] = list(client.service.extract_hashtag(t["text"]))
    return results

def call_sentiment_analysis(tweets, results, **kwargs):
    client = Client("http://127.0.0.1:8000/sentiment_service?wsdl")
    for t in tweets:
        if not t["id"] in results:
            results[t["id"]] = {} 
        results[t["id"]]["sentiment"] = client.service.sentiment_analysis(t["text"])
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
    'InPoDa',
    default_args=default_args,
    description='My WSDL calling DAG',
    schedule_interval=timedelta(days=1),
)

tweets = [t for t in tweets_collection.find()]
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
