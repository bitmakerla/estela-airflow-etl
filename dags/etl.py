import json
import os
import pendulum

#from kafka import KafkaConsumer
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.providers.apache.kafka.hooks.consume import KafkaConsumerHook

from airflow.decorators import task, dag
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.python import PythonOperator

now = pendulum.now()

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "host.docker.internal:29092")

def split_list(lst, batch_size):
    for i in range(0, len(lst), batch_size):
        yield lst[i:i + batch_size]

default_args = {
    'start_date': days_ago(1),
}

@dag(start_date=now, schedule=None, catchup=False)
def etl():
    @task
    def consume(**kwargs): 
        cnt = 0
        batch_size = int(kwargs["params"]["batch_size"])
        item_kafka_topic = kwargs["params"]["topic"]
        kafka_hook = KafkaConsumerHook([item_kafka_topic], "estela-kafka")
        consumer = kafka_hook.get_consumer()

        print("Getting the following conf %s", str(kwargs["params"]))
        message_list = []
        while True:
            if cnt >= batch_size:
                break;
            message = consumer.poll(2.0)
            if message is None:
                continue
            if message.error():
                print(f"Consumer error: {message.error()}")
                continue
            cnt += 1
            print(message)
            message_list.append(message.value().decode("utf-8"))
            print(f"Received message: {message.value()}")

        return message_list

    @task  # we expect to get a json items
    def transform(items):
        item_list = []
        for item in items:
            item_json = json.loads(item)
            item_list.append(item_json)

        return item_list
    
    @task
    def uploading_mongodb(items, **kwargs):
        #topic = kwargs["params"]["topic"]
        mongo_conn_id = kwargs["params"].get("mongo_conn_id", "estela-primary")
        # Database and collection should be defined in kwargs.
        mongo = MongoHook(mongo_conn_id=mongo_conn_id)
        client = mongo.get_conn()
        # job_id, spider_id, project_id, data_kind = topic.split(".")
        database_str = kwargs["params"]["mongo_database"]
        database = client.get_database(database_str)
        collection = database.get_collection(kwargs["params"]["mongo_collection"])
        inserted = collection.insert_many([item["payload"] for item in items])
        if len(inserted.inserted_ids) == len(items):
            outcome = "All documents were successfully inserted."
        else:
            outcome = "Not all documents were successfully inserted."
        return outcome  

    data = consume()
    items = transform(data)
    uploading_mongodb(items)

etl()

# with DAG(
#     dag_id="mongodb_read_dag",
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
#     def reading_mongodb():
#         mongo = MongoHook(mongo_conn_id="estela-primary")
#         client = mongo.get_conn()
#         database = client.get_database("185c81f4-bc89-41c6-90f6-99b8eef7d876")
#         collection = database.get_collection("129-210-job_items")
#         print("Reading from MongoDB: ")
#         print([item for item in collection.find()])
#     reading_mongodb = PythonOperator(
#         task_id="reading_mongodb",
#         python_callable=reading_mongodb,
#         provide_context=True,
#     )
#     reading_mongodb
