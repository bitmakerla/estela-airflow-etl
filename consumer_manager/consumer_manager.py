import json
import requests
import base64

from confluent_kafka import Consumer, Producer

AIRFLOW_API = "http://localhost:8080"

class ConsumerProxy:
    internal_queues = {}
    internal_cnt = {}
    batch_size = 200

    @staticmethod
    def delivery_report(err, msg):
        """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
        if err is not None:
            print('Message delivery failed: {}'.format(err))
        else:
            print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

    def __init__(self, conf):
        conf = {
            "bootstrap.servers": "localhost:29092",
            "group.id": "consumer_manager",
            "auto.offset.reset": "latest",
        }
        self.conf = conf
        self.consumer = Consumer(self.conf)
        self.consumer.subscribe(["^job_.*"])
        self.producer = Producer(self.conf)
    

    def process_message(self, msg):
        dsd_msg = json.loads(msg.value().decode("utf-8"))
        topic = msg.topic()
        jid = dsd_msg["jid"]
        if self.internal_queues.get(f"{jid}-{topic}", None) is None:
            self.internal_queues[f"{jid}-{topic}"] = []
            self.internal_cnt[f"{jid}-{topic}"] = 0
        else:
            self.internal_queues[f"{jid}-{topic}"].append(dsd_msg)
            print(f"Queue size for {jid}-{topic}: {len(self.internal_queues[f'{jid}-{topic}'])}")
        if len(self.internal_queues[f"{jid}-{topic}"]) >= self.batch_size:
            for item in self.internal_queues[f"{jid}-{topic}"]:
                self.producer.poll(0)
                self.producer.produce(f"{jid}-{topic}-{self.internal_cnt[f'{jid}-{topic}']}", json.dumps(item).encode("UTF-8"), callback=self.delivery_report)
            self.internal_queues[f"{jid}-{topic}"] = []
            self.producer.flush()
            job_id, spider_id, project_id = jid.split(".")
            payload = {
                "conf": {
                    "topic": f"{jid}-{topic}-{self.internal_cnt[f'{jid}-{topic}']}",
                    "batch_size": self.batch_size,
                    "mongo_database": project_id,
                    "mongo_collection": f"{job_id}-{spider_id}-{topic}",
                }
            }
            self.internal_cnt[f"{jid}-{topic}"] = self.internal_cnt[f"{jid}-{topic}"] + 1
            # Username and password for Airflow
            username = "airflow"
            password = "airflow"

            # Create a basic authentication header
            credentials = f"{username}:{password}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            headers = {
                "Content-Type": "application/json",
                # If authentication is required:
                "Authorization": f"Basic {encoded_credentials}"
            }
            path = "/api/v1/dags/etl/dagRuns"
            print(f"Triggering DAG: {AIRFLOW_API}{path}")
            response = requests.post(f"{AIRFLOW_API}{path}", headers=headers, data=json.dumps(payload)) 
            # Check the response
            if response.status_code == 200:
                print("DAG triggered successfully:", response.json())
            else:
                print(f"Failed to trigger DAG: {response.status_code} - {response.text}")
    
    def consume(self):
        while True:
            msg = self.consumer.poll(20.0)
            if msg is None:
                continue
            if msg.error():
                print("Consumer error: {}".format(msg.error()))
                continue
            self.process_message(msg)
            #print("Received message: {}".format(msg.value().decode("utf-8")))

if __name__ == "__main__":
    consumer_manager = ConsumerProxy({})
    consumer_manager.consume()
