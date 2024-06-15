from elasticsearch import Elasticsearch
from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, lit, when
from pyspark.sql.types import StringType
import pandas as pd
from elasticsearch.helpers import scan
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

es = Elasticsearch(
        "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
        api_key=api_key_metro_station
    )

# Extraction des noms d'arrêts de perimeter_df
stopnames_list = perimeter_df.select('ns3_stopname').rdd.flatMap(lambda x: x).collect()

# Ajouter les colonnes supplémentaires au DataFrame
for col_name in ["maps", "longitude", "latitude"] + [str(i) for i in range(1, 10)]:
    perimeter_df = perimeter_df.withColumn(col_name, lit(None).cast(StringType()))

# Interrogation d'Elasticsearch et mise à jour de perimeter_df
for element in stopnames_list[:50]:
    try:
        metro_station_output = elasticsearch_query(es, "metro_station", element, "nom")[0]
        maps_value = metro_station_output.get("maps")
        additional_columns = {str(i): metro_station_output.get(str(i)) for i in range(1, 10)}

        # Mise à jour du DataFrame
        perimeter_df = perimeter_df.withColumn("maps", 
                                               when(col("ns3_stopname") == element, lit(maps_value)).otherwise(col("maps")))
        for col_name, col_value in additional_columns.items():
            perimeter_df = perimeter_df.withColumn(col_name, 
                                                   when(col("ns3_stopname") == element, lit(col_value)).otherwise(col(col_name)))

    except Exception as e:
        print(f"Error processing element {element}: {e}")


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