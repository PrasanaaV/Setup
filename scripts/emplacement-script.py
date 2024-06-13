import json
from elasticsearch import Elasticsearch

def fetch_data_from_json():
    print('Fetching data from JSON')
    with open('../files/emplacement-des-gares-idf.json', 'r') as file:
        content = json.load(file)

    simplified_records = []
    for record in content:
        picto = record.get('picto')
        simplified_record = {
            'id_gares': record.get('id_gares'),
            'nom_gares': record.get('nom_gares'),
            'longitude': record.get('geo_point_2d', {}).get('lon'),
            'latitude': record.get('geo_point_2d', {}).get('lat'),
            'res_com': record.get('res_com'),
            'mode': record.get('mode'),
            'exploitant': record.get('exploitant'),
            'picto': picto['filename'] if picto else None
        }
        simplified_records.append(simplified_record)
    return simplified_records

def insert_into_elasticsearch(content):
    client = Elasticsearch(
        "https://e7bce3a6df364700956f06c04d5fc655.us-central1.gcp.cloud.es.io:443",
        api_key="VTV1amhvOEJZa2MtZnhCNzRSS0s6MzdGMUpUUmtRMUNpdG1JbTRKbGFOUQ=="
    )
    
    for item in content:
        unique_id = f"{item['id_gares']}_{item['nom_gares']}"
        client.index(index="emplacement-stations-idf", id=unique_id, body=item)
    print("New data inserted successfully.")
    return

content = fetch_data_from_json()
insert_into_elasticsearch(content)