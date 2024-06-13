import json
from elasticsearch import Elasticsearch

def fetch_data_functions_stop_lines():
    print('Hello from fetch data DAG')
    with open('../files/arrets-lignes.json', 'r') as file:
        content = json.load(file)

    simplified_records = []
    for record in content:
        fields = record.get('fields')
        if fields:
            simplified_record = {
                'route_long_name': fields.get('route_long_name'),
                'stop_id': fields.get('stop_id'),
                'stop_name': fields.get('stop_name'),
                'nom_commune': fields.get('nom_commune')
            }
            simplified_records.append(simplified_record)

    return simplified_records

def insert_into_elasticsearch(content):
    client = Elasticsearch(
    "https://e7bce3a6df364700956f06c04d5fc655.us-central1.gcp.cloud.es.io:443",
    api_key="VTV1amhvOEJZa2MtZnhCNzRSS0s6MzdGMUpUUmtRMUNpdG1JbTRKbGFOUQ=="
    )
    
    for item in content:
        unique_id = f"{item['route_long_name']}_{item['stop_id']}"
    
        client.index(index="stop-lines", id=unique_id, body=item)
    print("New data inserted successfully.")
    return


content = fetch_data_functions_stop_lines()
insert_into_elasticsearch(content)

