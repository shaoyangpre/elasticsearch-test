import json
import uuid

import requests

headers = {
        "Content-type": 'application/json'
    }

def search(url, query_body):
    rep = requests.post(url, json=query_body, headers=headers).json()
    data = [doc["_source"] for doc in rep['hits']['hits']]

    return data

def bulk(url):
    bulk_data = [
        {"index": {"_index": "my_store", "_type": "_doc", "_id": str(uuid.uuid4())}},
        {"price": 10, "productID": "1111"},
        {"index": {"_index": "my_store", "_type": "_doc", "_id": str(uuid.uuid4())}},
        {"price": 20, "productID": "1112"}
    ]
    format_data  = '\n'.join([json.dumps(data) for data in bulk_data]) + '\n'
    rep = requests.post(url, data=format_data, headers=headers).json()
    if not rep['errors']:
        return '批量处理成功'
    else:
        return '批量处理失败'




if __name__ == "__main__":
    url = 'http://127.0.0.1:9200/my_store/_bulk'
    print(bulk(url))
