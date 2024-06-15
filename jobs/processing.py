from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when
from elasticsearch.helpers import scan
import pandas as pd
from confluent_kafka import Consumer, KafkaException, KafkaError
import json

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Word Count") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# API keys for each index
api_key_metro_station = "TWxtbTdZOEItODFDdXJHT2ptcXI6VkdHWl9kcF9UZmk3ZS05dldMVVV4Zw=="
api_key_perimeter = "QzFsdkc1QUItODFDdXJHT1FuR2g6ZDNxbWdHVXVSQmFzZlZyYW5qSEw4UQ=="
api_key_emplacement_stations_idf = "bkZWbjU0OEJKR3c1U0ExQWJMQXc6RVRESFRDOC1UMTJNMm5lc19CMXFnQQ=="

es = Elasticsearch(
        "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
        api_key=api_key_perimeter
    )

query = {
    "query": {
        "match_all": {}
    },
    "size": 1000
}
res = es.search(index="perimeter", body=query)
data = [hit['_source'] for hit in res['hits']['hits']]
df_pd = pd.DataFrame(data)
perimeter_df = spark.createDataFrame(df_pd)

def elasticsearch_query(es_client, index, query, field):
    body = {
        "query": {
            "simple_query_string": {
                "query": query,
                "fields": [field]
            }
        }
    }
    results = scan(es_client, index=index, query=body)
    return [hit["_source"] for hit in results]

metro_station_es = Elasticsearch(
    "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
    api_key=api_key_metro_station
)

emplacement_stations_idf_es = Elasticsearch(
    "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
    api_key=api_key_emplacement_stations_idf
)

# Extraction des noms d'arrêts de perimeter_df
stopnames_list = perimeter_df.select('ns3_stopname').distinct().rdd.flatMap(lambda x: x).collect()

# Batch process Elasticsearch queries
metro_station_results = {}
for element in stopnames_list:
    try:
        metro_station_results[element] = elasticsearch_query(metro_station_es, "metro_station", element, "nom")[0]
    except Exception as e:
        print(f"Error processing element {element}: {e}")

# Create Spark DataFrame
columns = ["ns3_stopname", "maps"] + [str(i) for i in range(1, 10)]
metro_station_updates = [
    (k, v.get("maps"), *[v.get(str(i)) for i in range(1, 10)]) 
    for k, v in metro_station_results.items()
]
metro_station_df = spark.createDataFrame(metro_station_updates, columns)

# Batch process Elasticsearch queries for the second table
emplacement_stations_idf_results = {}
for element in stopnames_list:
    try:
        result = elasticsearch_query(emplacement_stations_idf_es, "emplacement-stations-idf", element, "nom_gares")[0]
        emplacement_stations_idf_results[element] = result
    except Exception as e:
        print(f"Error processing element {element}: {e}")

# Create Spark DataFrame
columns = ["ns3_stopname", "longitude", "latitude"]
emplacement_stations_idf_updates = [
    (k, v.get("longitude"), v.get("latitude"))
    for k, v in emplacement_stations_idf_results.items()
]

emplacement_stations_idf_df = spark.createDataFrame(emplacement_stations_idf_updates, columns)

# Join the updates with the original DataFrame
perimeter_df = perimeter_df.join(metro_station_df, on="ns3_stopname", how="left") \
                           .join(emplacement_stations_idf_df, on="ns3_stopname", how="left")

# Afficher les lignes contenant des valeurs nulles
# null_rows_df = perimeter_df.filter(col("maps").isNull() | col("longitude").isNull())
# null_rows_df.show()

# perimeter_df.show(len(stopnames_list), truncate=False)


def get_last_message_with_key():
    broker = 'kafka1:29092'  # Adresse du broker Kafka
    group = 'metro-group'       # ID du groupe de consommateurs
    topic = 'infosRealTime'  # Topic Kafka à consommer
    key = 'STIF:StopPoint:Q:22061'           # Clé à filtrer

    # Configuration pour le consommateur Kafka
    conf = {
        'bootstrap.servers': broker,
        'group.id': group,
        'auto.offset.reset': 'latest'  # Pour obtenir les derniers messages
    }
    
    # Création de l'instance du consommateur
    consumer = Consumer(conf)
    
    # S'abonner au topic
    consumer.subscribe([topic])
    
    while True:
        # Poll pour un message
        msg = consumer.poll(timeout=1.0)
        print(msg)
        
        # Message correct
        message_key = msg.key().decode('utf-8') if msg.key() else None
        message_value = msg.value().decode('utf-8')
        
        if message_key == key:
            message_data = json.loads(message_value)
            print(f'Received message with key {key}: {message_data}')
            consumer.close()
            return message_data
                
    
last_message = get_last_message_with_key()
if last_message:
    print(f'Dernier message avec la clé STIF:StopPoint:Q:22061: {last_message}')
else:
    print(f'Pas de message trouvé avec la clé STIF:StopPoint:Q:22061')

spark.stop()