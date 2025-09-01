#!/usr/bin/env python3
import json
import yaml
import requests
from requests.auth import HTTPBasicAuth

# Simple test index body first
test_body = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "doc_type": {"type": "keyword"},
            "title": {"type": "text", "analyzer": "ik_max_word"},
            "content": {"type": "text", "analyzer": "ik_smart"}
        }
    }
}

print("Testing with simple mapping...")
response = requests.put(
    "http://elasticsearch:9200/test-cross-search",
    json=test_body,
    auth=HTTPBasicAuth('elastic', 'admin@12345')
)

print(f"Test Status: {response.status_code}")
if response.status_code != 200:
    print(f"Error: {response.text}")
else:
    print("Success! Deleting test index...")
    requests.delete(
        "http://elasticsearch:9200/test-cross-search",
        auth=HTTPBasicAuth('elastic', 'admin@12345')
    )

# Now try with the actual config
print("\n" + "="*50)
print("Loading actual config...")

with open('/app/config/sync/cross_search_template.yaml', 'r') as f:
    config = yaml.safe_load(f)

index_config = config['cross_search']
index_body = {
    "settings": index_config['settings'],
    "mappings": index_config['mappings']
}

print(f"Creating index: {index_config['index_name']}")
print("Settings keys:", list(index_config['settings'].keys()))
print("Mappings properties:", list(index_config['mappings']['properties'].keys()))

response = requests.put(
    f"http://elasticsearch:9200/{index_config['index_name']}",
    json=index_body,
    auth=HTTPBasicAuth('elastic', 'admin@12345')
)

print(f"Status: {response.status_code}")
if response.status_code != 200:
    error = response.json()
    print("Error details:")
    print(json.dumps(error, indent=2))
