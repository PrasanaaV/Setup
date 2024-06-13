import json
from elasticsearch import Elasticsearch

def fetch_data_functions_stations():
    print('Hello from fetch data DAG')
    with open('files/perimetre-des-donnees-tr-disponibles-plateforme-idfm.json', 'r') as file:
        content = json.load(file)

    simplified_records = []
    for record in content:
        fields = record.get('fields')
        if fields:
            simplified_record = {
                'line': fields.get('line'),
                'ns3_stoppointref': fields.get('ns3_stoppointref'),
                'name_line': fields.get('name_line'),
                'ns3_stopname': fields.get('ns3_stopname')
            }
            simplified_records.append(simplified_record)

    return simplified_records    

def insert_into_elasticsearch(content):
    client = Elasticsearch(
    "https://e7bce3a6df364700956f06c04d5fc655.us-central1.gcp.cloud.es.io:443",
    api_key="VTV1amhvOEJZa2MtZnhCNzRSS0s6MzdGMUpUUmtRMUNpdG1JbTRKbGFOUQ=="
    )
    
    for item in content:
        unique_id = f"{item['line']}_{item['ns3_stoppointref']}"
    
        # Insert the document using the unique identifier
        client.index(index="perimetre", id=unique_id, body=item)
    print("New data inserted successfully.")
    return