from kafka import KafkaProducer
import json
import requests
from datetime import datetime

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

def parse_expected_arrival_time(expected_arrival_time):
    try:
        return datetime.fromisoformat(expected_arrival_time.replace("Z", "+00:00"))
    except ValueError:
        return None

def extract_closest_arrival_times(data):
    closest_times = {}

    for delivery in data['Siri']['ServiceDelivery']['EstimatedTimetableDelivery']:
        for frame in delivery['EstimatedJourneyVersionFrame']:
            for journey in frame['EstimatedVehicleJourney']:
                for call in journey['EstimatedCalls']['EstimatedCall']:
                    stop_point_ref = call['StopPointRef']['value']
                    expected_arrival_time_str = call.get('ExpectedArrivalTime', call.get('ExpectedDepartureTime'))
                    expected_arrival_time = parse_expected_arrival_time(expected_arrival_time_str)
                    destination_display = call['DestinationDisplay'][0]['value'] if 'DestinationDisplay' in call and call['DestinationDisplay'] else 'N/A'

                    if stop_point_ref not in closest_times or (expected_arrival_time and expected_arrival_time < parse_expected_arrival_time(closest_times[stop_point_ref]['ExpectedArrivalTime'])):
                        closest_times[stop_point_ref] = {
                            'ExpectedArrivalTime': expected_arrival_time_str,
                            'DestinationDisplay': destination_display
                        }

    return list(closest_times.items())

def send(key, value, producer):
    print(key)
    print(value)
    try:
        key_bytes = key.encode('utf-8')
        value_bytes = json.dumps(value).encode('utf-8')
        producer.send('infosRealTime', key=key_bytes, value=value_bytes)
        print("Message sent successfully.")
    except Exception as e:
        print(f"Error sending message: {e}")

def kafka_fct():
    producer = KafkaProducer(bootstrap_servers=['kafka1:19092'])
    with open('jobs/mapping.json') as f:
        i = 0
        f = json.load(f)
        lstMapping = f.values()
        for station in lstMapping:
            res = realTime(station)
            if res:  # Add a check to ensure res is not None
                lst = extract_closest_arrival_times(res)
                for element in lst:
                    key, value = element
                    send(key, value, producer)
                    i += 1
        print(i)
    producer.flush()  # Ensure all messages are sent before closing
    producer.close()