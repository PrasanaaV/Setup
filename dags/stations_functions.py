from bs4 import BeautifulSoup
import requests
import time
import json
from elasticsearch import Elasticsearch
from airflow.providers.elasticsearch.hooks.elasticsearch import ElasticsearchHook, ElasticsearchSQLHook

def create_info_station():
    print('Hello')
    numbers = ["ligne" + str(i) for i in range(1,13)]
    numbers.append('lignea')
    numbers.append('ligneb')
    numbers.append('ligne7bis')
    numbers.append('ligne3bis')
    lstToAvoid = ['search.php', 'mentions-legales.php', 'le-guichet.php', 'contact.php']
    numberPhp = [number + ".php" for number in numbers]
    lstToAvoid.extend(numberPhp)
    res = {}

    for number in numbers :
        url = f'https://www.sortiesdumetro.fr/{number}.php'
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, "html.parser")
            links = [link.get('href') for link in soup.find_all('a')]
            links = list(set(links))
            for link in links:
                if link:
                    if not (link.startswith('https') or link in lstToAvoid):
                        name = link[:len(link)-4]
                        if not name in res:
                            newUrl = 'https://www.sortiesdumetro.fr/' + link
                            innerHashMap = retrieveInfosSortieMetro(newUrl, number)
                            res[name] = innerHashMap
        else:
            print(url + " : Problem getting sortiesdumetro.fr")
    return res

def retrieveInfosSortieMetro(url, number):
    retry = 0
    innerHashMap = {}

    response = requests.get(url)
    html_content = response.text

    soup = BeautifulSoup(html_content, "html.parser")
    iframe = soup.find("iframe")
    if iframe:
        src = iframe.get("src")
        elements = soup.find_all("li", id=True)
        for element in elements:
            text = element.get_text()
            num = text[0]
            text = text[2:]
            innerHashMap[num] = text
            innerHashMap["number"] = number
        innerHashMap["maps"] = src
        return innerHashMap
    else:
        while retry <= 1:
            retry += 1
            time.sleep(2)
            retrieveInfosSortieMetro(url)

def elastic_metro_station():
    res = create_info_station()
    client = Elasticsearch(
        "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
        api_key="TWxtbTdZOEItODFDdXJHT2ptcXI6VkdHWl9kcF9UZmk3ZS05dldMVVV4Zw==",
        timeout=30,           # Connection timeout
        max_retries=3,        # Maximum number of retries on connection failure
        retry_on_timeout=True  # Retry on connection timeout
    )

    for key, value in res.items():
        value['nom'] = key
        client.index(index="metro_station", id=key, body=value)
    print("New data inserted successfully.")
    return
    
def fetch_data_from_json_emplacement_stations():
    print('Fetching data from JSON')
    with open('dags/files/emplacement-des-gares-idf.json', 'r') as file:
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

def insert_into_elasticsearch_emplacement_stations(content):

    client = Elasticsearch(
        "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
        api_key="bkZWbjU0OEJKR3c1U0ExQWJMQXc6RVRESFRDOC1UMTJNMm5lc19CMXFnQQ==",
        timeout=30,           # Connection timeout
        max_retries=3,        # Maximum number of retries on connection failure
        retry_on_timeout=True  # Retry on connection timeout
    )
        
    for item in content:
        unique_id = f"{item['id_gares']}_{item['nom_gares']}"
        client.index(index="emplacement-stations-idf", id=unique_id, body=item)
    print("New data inserted successfully.")
    return

def fetch_and_insert_into_elasticsearch_emplacement_stations():
    content = fetch_data_from_json_emplacement_stations()
    insert_into_elasticsearch_emplacement_stations(content)

def fetch_data_from_json_perim():
    print('Hello from fetch data DAG')
    with open('dags/files/perimetre-des-donnees-tr-disponibles-plateforme-idfm.json', 'r') as file:
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
    

def insert_into_elasticsearch_perim(content):
    
    client = Elasticsearch(
        "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
        api_key="QzFsdkc1QUItODFDdXJHT1FuR2g6ZDNxbWdHVXVSQmFzZlZyYW5qSEw4UQ==",
        timeout=30,           # Connection timeout
        max_retries=3,        # Maximum number of retries on connection failure
        retry_on_timeout=True  # Retry on connection timeout
    )

    lst = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "A", "B", "3B", "7B"]
   
    for item in content:
        if item['name_line'] in lst:
            unique_id = f"{item['line']}_{item['ns3_stoppointref']}"
            # Insert the document using the unique identifier
            client.index(index="perimeter", id=unique_id, body=item)
    print("New data inserted successfully.")
    return

def fetch_and_insert_into_elasticsearch_perim():
    content = fetch_data_from_json_perim()
    insert_into_elasticsearch_perim(content)

#---

def get_data_gare_du_nord():
    client = Elasticsearch(
        "https://7f0ef8badf50482b9b0d93ef141e14ec.us-central1.gcp.cloud.es.io:443",
        api_key="bkZWbjU0OEJKR3c1U0ExQWJMQXc6RVRESFRDOC1UMTJNMm5lc19CMXFnQQ==",
        timeout=30,           # Connection timeout
        max_retries=3,        # Maximum number of retries on connection failure
        retry_on_timeout=True  # Retry on connection timeout
    )

    response = client.search(
    index="emplacement-stations-idf",
    body={
        "query": {
            "match": {
                "nom_gares": "Gare du Nord"
                }
              }
            }
        )
    # Les documents correspondants seront dans 'hits' dans la réponse
    documents = response['hits']['hits']
    print(documents)