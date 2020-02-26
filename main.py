import pandas as pd
import requests
from requests_aws4auth import AWS4Auth
import boto3

url = 'https://my.elasticsearch.domain/my_awesome_index/_search'
service = 'es'

credentials = boto3.Session().get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, AWS_REGION, service, session_token=credentials.token)

def search(query):
    # executes the query over the search url
    headers = {"Content-Type": "application/json"}
    r = requests.get(url, auth=awsauth, headers=headers, data=json.dumps(query))
    return json.loads(r.text)


def parseData(esResponse):
    # unpack values from elastic search response object if any
    dataArr = []
    rows = esRes['hits']['hits']
    for row in rows:
	data = row['_source']
	data['id'] = row['id']
        dataArr.append(row['_source'])
   return dataAtt


def getData():
    # Put the user query into the query DSL for more accurate search results.
    # Note that range is using date range for last 1 year of record.
    # sort is necessary
    
    query = {
        "query": {
                "match_all": {}
        },
        "sort": [
            {
                "createdAt": {
                    "order": "desc"
                }
            },
            {
                "id": {
                    "order": "desc"
                }
            }
        ],
        "size": 5000
    }
    results = []
    # ES 6.x requires an explicit Content-Type header
    # Make the signed HTTP request
    esRes = search(url, query)

    results = results + parseData(esRes)

    while len(esRes['hits']['hits']) > 0:
        resLen = len(esRes['hits']['hits'])
        query['search_after'] = esRes['hits']['hits'][resLen-1]['sort']
        esRes = search(url, query)
        results = results + parseData(esRes)

    return results
