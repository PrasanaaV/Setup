from kafka import KafkaProducer
import json
import requests

def realTime(LineRef):
    # URL de l'API
    url = "https://prim.iledefrance-mobilites.fr/marketplace/estimated-timetable"

    # Votre clé API
    api_key = "VKl2FLcQCLZXZxJfP65faKkRRxSHWdHX"

    # En-têtes de la requête
    headers = {
        "apikey": api_key
    }

    params = {
        "LineRef": LineRef  # Vous pouvez changer cette valeur en fonction de la ligne que vous souhaitez interroger
    }

    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        return response.json()

def extract_info(data):
    extracted_info = []
    for delivery in data['Siri']['ServiceDelivery']['EstimatedTimetableDelivery']:
        for frame in delivery['EstimatedJourneyVersionFrame']:
            for journey in frame['EstimatedVehicleJourney']:
                for call in journey['EstimatedCalls']['EstimatedCall']:
                    key = call['StopPointRef']['value']
                    info ={'ExpectedArrivalTime': call.get('ExpectedArrivalTime', 'N/A'), 
                             'DestinationDisplay' : call['DestinationDisplay'][0]['value'] 
                                if 'DestinationDisplay' in call and call['DestinationDisplay'] 
                                else 'N/A'
                            }
                    extracted_info.append((key,info))
    return extracted_info

def send(key, value, producer):
    try:
        producer.send('infosRealTime', key=key, value=value)
        print("Message sent successfully.")
    except Exception as e:
        print(f"Error sending message: {e}")

def kafka_fct():
    producer = KafkaProducer(bootstrap_servers=['kafka1:19092'])
    with open('jobs/mapping.json') as f:
        f = json.load(f)
        lstMapping = f.values()
        for station in lstMapping:
            res = realTime(station)
            lst = extract_info(res)
            for element in lst:
                key, value = element
                send(key, value, producer)
    producer.flush()  # Ensure all messages are sent before closing
    producer.close()