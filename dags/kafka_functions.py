from kafka import KafkaProducer
import json
from elasticsearch import Elasticsearch
import requests

def get_estimated_timetable(stop_point_ref):
    url = "https://prim.iledefrance-mobilites.fr/marketplace/stop-monitoring"

    params = {
        "MonitoringRef": stop_point_ref
    }

    headers = {
        "apikey": "VKl2FLcQCLZXZxJfP65faKkRRxSHWdHX"
    }

    # Effectuer la requête GET
    response = requests.get(url, params=params, headers=headers)

    # Vérifier si la requête a réussi
    if response.status_code == 200:
        data = response.json()  
        return data
    else:
        print("Failed to fetch data: Status code", response.status_code)
        return None

def kafka_fct():
    es = Elasticsearch(
        "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
        api_key="UWxrRTdvOEItODFDdXJHT3hHcUQ6Z0FobmRTMVVRcWVud01jbVozREFodw==",
        timeout=30,           # Connection timeout
        max_retries=3,        # Maximum number of retries on connection failure
        retry_on_timeout=True  # Retry on connection timeout
    )

    query = {
    "query": {
        "match_all": {}
        }
    }
    index_name = "output"

    # Récupérer les documents de l'index
    response = es.search(index=index_name, body=query, size=1000)

    # Initialiser un dictionnaire pour stocker les résultats
    result_dict = {}

    # Parcourir les hits (documents) retournés par la requête
    for hit in response['hits']['hits']:
        # Extraire les champs nom et stopref de chaque document
        nom = hit['_source'].get('nom')
        stopref = hit['_source'].get('stopref')

        data = get_estimated_timetable(stopref)
        destination_display = data['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit'][0]['MonitoredVehicleJourney']['MonitoredCall']['DestinationDisplay'][0]['value']
        expected_arrival_time = data['Siri']['ServiceDelivery']['StopMonitoringDelivery'][0]['MonitoredStopVisit'][0]['MonitoredVehicleJourney']['MonitoredCall']['ExpectedArrivalTime']

        res = dict({"stopRef": {"destination": destination_display,"expected_arrival_time":expected_arrival_time }})

        # Ajouter les données au dictionnaire
        if nom is not None and stopref is not None:
            result_dict[nom] = stopref

    # Afficher le dictionnaire résultant
    print(result_dict)

    send(result_dict)

def send(res):
    try:
        producer = KafkaProducer(bootstrap_servers=['kafka1:19092'])
        producer.send('infosRealTime', json.dumps(res).encode('utf-8'))
        producer.flush()  # Ensure all messages are sent before closing
        producer.close()
        print("Message sent successfully.")
    except Exception as e:
        print(f"Error sending message: {e}")