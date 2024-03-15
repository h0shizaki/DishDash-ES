from bson import ObjectId
from elasticsearch import Elasticsearch
import os
from pathlib import Path
import json

import time
from flask import Flask, request, jsonify
import pandas as pd
import pymongo
from flask_cors import CORS

from utils import dataframe_parser, get_paginated_response, get_queries_from_user

app = Flask(__name__)
CORS(app)
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
    response = get_paginated_response(results, '/browse?query=' + query_term + '&search_size=' + str(search_size),
                                      total=len(results["hits"]["hits"]),
                                      start=request.args.get('start', 1),
                                      limit=request.args.get('limit', 20))
    response['status'] = 'success'

    response['total_hit'] = total_hit
    # res['results'] = results_df.to_dict("records")
    # res['results'] = jsonify(results_df)
    response['elapse'] = end - start
    return response

@app.route('/explore', methods=["GET"])
def explore():
    start = time.time()
    search_size = request.args.get('search_size', 100)
    query = {
        "function_score": {
            "query": {"match_all": {}},
            "random_score": {}
        }
    }
    results = app.es_client.search(index='recipe', source_excludes=['url_lists'], size=search_size, query=query)
    total_hit = results['hits']['total']['value']
    end = time.time()
    response = get_paginated_response(results, '/explore?' + 'search_size=' + str(search_size),
                                      total=len(results["hits"]["hits"]),
                                      start=request.args.get('start', 1),
                                      limit=request.args.get('limit', 20))
    response['status'] = 'success'

    response['total_hit'] = total_hit
    # res['results'] = results_df.to_dict("records")
    # res['results'] = jsonify(results_df)
    response['elapse'] = end - start
    return response


@app.route('/recipe', methods=["GET"])
def browse():
    start = time.time()
    user_id = request.args.get('_id')
    search_size = request.args.get('search_size', 100)
    users = app.db['users']
    user = users.find_one({'_id': ObjectId(user_id)})
    if user == None:
        return {'message': 'User Not Found', 'status': '404'}

    print(user['username'], user['interestedCategory'], user['interestedRecipe'], user['uninterestedRecipe'])

    query = get_queries_from_user(user)

    results = app.es_client.search(index='recipe', query=query, size=search_size)
    total_hit = results['hits']['total']['value']
    end = time.time()
    response = get_paginated_response(results,
                                      '/search_recipe?_id=' + str(user_id) + '&search_size=' + str(search_size),
                                      total=len(results["hits"]["hits"]),
                                      start=request.args.get('start', 1),
                                      limit=request.args.get('limit', 20))
    response['status'] = 'success'

    response['total_hit'] = total_hit
    # res['results'] = results_df.to_dict("records")
    # res['results'] = jsonify(results_df)
    response['elapse'] = end - start
    return response


@app.route('/recipe/<recipe_id>', methods=["GET"])
def recipe(recipe_id):
    start = time.time()
    suggest_size = request.args.get('suggest_size', 4)

    recipe_db = app.db['recipes']
    result = recipe_db.find_one({'_id': ObjectId(recipe_id)})

    end = time.time()
    response = {'elapse': end - start}

    if result is None:
        response['status'] = '404'
        response['message'] = 'Recipe not found'
        return response

    query = {
        "more_like_this": {
            # Breakfast Eggcake ID:'65d5e4928598535be43ec668'
            "fields": ["Name", "Keywords", "RecipeIngredientParts", "RecipeCategory"],
            "like": [{"_id": recipe_id}], "min_term_freq": 1, "min_doc_freq": 5, "max_query_terms": 20
        }
    }

    suggestions = app.es_client.search(index='recipe', query=query, size=suggest_size)
    suggestions_df = dataframe_parser(suggestions)

    result['_id'] = recipe_id
    response['result'] = result
    response['suggestions'] = suggestions_df.to_dict("records")
    response['elapse'] = end - start
    response['status'] = 'success'

    return response


if __name__ == '__main__':
    app.run(debug=False)
