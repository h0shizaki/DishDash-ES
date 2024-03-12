from bson import ObjectId
from elasticsearch import Elasticsearch
import os
from pathlib import Path
import json

import time
from flask import Flask, request, jsonify
import pandas as pd
import pymongo

from utils import dataframe_parser, get_paginated_response

app = Flask(__name__)
app.es_client = Elasticsearch('https://localhost:9200', basic_auth=("elastic", "6E0GWL_MEddnKJWCnk*M"),
                              ca_certs="./http_ca.crt")
app.mongo_client = pymongo.MongoClient("mongodb://root:123456@localhost:27017/?authMechanism=DEFAULT")
app.db = app.mongo_client["IR"]


@app.route('/search_recipe', methods=["GET"])
def search_recipe():
    start = time.time()
    # res = {'status': 'success'}
    arg_list = request.args.to_dict(flat=False)
    query_term = arg_list['query'][0]
    search_size = request.args.get('search_size', 100)
    query = {
        "query_string": {
            "query": query_term
        }
    }
    results = app.es_client.search(index='recipe', source_excludes=['url_lists'], size=search_size, query=query)
    total_hit = results['hits']['total']['value']
    end = time.time()
    response = get_paginated_response(results, '/search_recipe?query=' + query_term + '&search_size=' + search_size,
                                      total=len(results["hits"]["hits"]),
                                      start=request.args.get('start', 1),
                                      limit=request.args.get('limit', 20))
    response['status'] = 'success'

    response['total_hit'] = total_hit
    # res['results'] = results_df.to_dict("records")
    # res['results'] = jsonify(results_df)
    response['elapse'] = end - start
    return response


@app.route('/browse', methods=["GET"])
def browse():
    start = time.time()
    user_id = request.args.get('_id')
    search_size = request.args.get('search_size', 100)
    users = app.db['users']
    user = users.find_one({'_id': ObjectId(user_id)})
    print(user['username'], user['interestedCategory'], user['interestedRecipes'], user['uninterestedRecipes'])

    return {start: start}


@app.route('/search_es', methods=["GET"])
def search_es():
    start = time.time()
    res = {'status': 'success'}
    argList = request.args.to_dict(flat=False)
    query_term = argList['query'][0]
    results = app.es_client.search(index='recipe', source_excludes=['url_lists'], size=100,
                                   query={"match": {"text": query_term}})
    end = time.time()
    total_hit = results['hits']['total']['value']
    results_df = pd.DataFrame([[hit["_source"]['title'], hit["_source"]['url'], hit["_source"]
                                                                                ['text'][:100], hit["_score"]] for hit
                               in results['hits']['hits']], columns=['title', 'url', 'text',
                                                                     'score'])

    res['total_hit'] = total_hit
    res['results'] = results_df.to_dict('records')
    res['elapse'] = end - start

    return res


if __name__ == '__main__':
    app.run(debug=False)
