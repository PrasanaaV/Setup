from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
import uuid
from datetime import datetime, timezone
import json

client = Elasticsearch(
        "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
        api_key="NDFrVElaQUItODFDdXJHT1duSFg6LWRfQ2JxeGVROHFvTGNWM0R1ZFM0UQ==",
        timeout=30,           # Connection timeout
        max_retries=3,        # Maximum number of retries on connection failure
        retry_on_timeout=True  # Retry on connection timeout
    )

documents = []
scan_results = scan(client=client, index='clean_index', query={"query": {"match_all": {}}})
for doc in scan_results:
    doc_with_id = doc["_source"]
    doc_with_id["_id"] = doc["_id"]
    documents.append(doc_with_id)

def loop_over(message):
    for element in documents:
        key = message.key.decode('utf-8')
        value = message.value.decode('utf-8')
        try:
            value = json.loads(message.value.decode('utf-8'))
        except json.JSONDecodeError:
            print(f"Error decoding JSON: {message.value}")
        if element['ns3_stoppointref'] == key : 
            
            minutes = -1
            seconds = "N/A"
            if value['ExpectedArrivalTime'] != "N/A":
                target_date = datetime.fromisoformat(value['ExpectedArrivalTime'].replace("Z", "+00:00"))
                current_date = datetime.now(timezone.utc)
                time_difference = target_date - current_date
                minutes,seconds = (time_difference.seconds % 3600) // 60, time_difference.seconds % 60

            element["waiting_time"] = minutes
            element["destination"] = value['DestinationDisplay']

            update_element = element.copy()
            update_element.pop('_id', None)
            print(update_element)
            client.update(
                index='clean_index',
                id=element["_id"],
                body={
                    "doc": update_element
                    }
            )
            print('updated')



def get_last_message_with_key():

    topic = 'infosRealTime'
    bootstrap_servers = ['kafka1:19092']
    unique_group_id = f'my-group-{uuid.uuid4()}'

    consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    auto_offset_reset='latest',  # Start from the earliest messages
    enable_auto_commit=True,
    group_id=unique_group_id,
)

    try:
        for message in consumer:
            loop_over(message)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

get_last_message_with_key()