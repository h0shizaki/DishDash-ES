import numpy as np
from bson import ObjectId
from elasticsearch import Elasticsearch
import time
from flask import Flask, request
import pymongo
from flask_cors import CORS
from langchain_community.document_loaders import TextLoader
from langchain_text_splitters import CharacterTextSplitter

from langchain_community.vectorstores import Chroma
from langchain_community.chat_models import ChatOpenAI
from langchain.chains.question_answering import load_qa_chain
from openai import OpenAI
from langchain_community.embeddings.sentence_transformer import (
    SentenceTransformerEmbeddings,
)

import config
from utils import dataframe_parser, get_paginated_response, get_queries_from_user, spell_correction_parser

app = Flask(__name__)
CORS(app)
app.es_client = Elasticsearch('https://localhost:9200', basic_auth=("elastic", "6E0GWL_MEddnKJWCnk*M"),
                              ca_certs="./http_ca.crt")
app.mongo_client = pymongo.MongoClient("mongodb://root:123456@localhost:27017/?authMechanism=DEFAULT")
app.openai_client = OpenAI(api_key=str(config.OPENAI_API_KEY))

embeddings = SentenceTransformerEmbeddings(model_name="all-MiniLM-L6-v2")
app.vec_db = Chroma(persist_directory="./chroma_db", embedding_function=embeddings)


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

    print(user['username'], user['interestedCategory'], user['interestedRecipe'], user['uninterestedCategory'])

    query = get_queries_from_user(user, 1, 1)

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
    response['elapse'] = end - start
    return response


@app.route('/bookmark/<bookmark_id>', methods=["GET"])
def bookmark_suggestion(bookmark_id):
    start = time.time()
    search_size = request.args.get('search_size', 44)
    bookmarks = app.db['bookmarks']
    bookmark = bookmarks.find_one({'_id': ObjectId(bookmark_id)})

    if bookmark is None:
        return {'message': 'User Not Found', 'status': '404'}

    # print(bookmark)
    records = (bookmark['records'])
    like = []
    for record in records:
        like.append({'_id': str(record['recipe'])})

    print(like)

    suggestion_query = {}
    if len(like) != 0:
        suggestion_query = {
            'more_like_this': {
                "fields": ["Keywords", "RecipeIngredientParts", "RecipeCategory"],
                "like": like, "min_term_freq": 1, "min_doc_freq": 5, "max_query_terms": 20,
            }
        }
    else:
        suggestion_query = {
            "query_string": {
                "query": bookmark['title']
            }
        }

    suggestions = app.es_client.search(index='recipe', query=suggestion_query, size=search_size)
    suggestions_df = dataframe_parser(suggestions)

    end = time.time()
    response = {'status': 'success', 'elapse': end - start,
                'suggestions': suggestions_df.to_dict("records")}

    return response


@app.route('/favorite', methods=["GET"])
def favorite_list():
    start = time.time()
    users = app.db['users']
    user_id = request.args.get('_id')
    user = users.find_one({'_id': ObjectId(user_id)})
    suggest_size = request.args.get('suggest_size', 24)
    if user == None:
        return {'message': 'User Not Found', 'status': '404'}
    recipes = set(user['interestedRecipe'])
    query = []
    for recipe in recipes:
        query.append(ObjectId(recipe))

    recipe_db = app.db['recipes']
    result = list(recipe_db.find({'_id': {'$in': query}}))
    like = []

    for doc in result:
        doc['_id'] = str(doc['_id'])
        like.append({'_id': doc['_id']})

    suggestion_query = {}
    if len(like) != 0:
        suggestion_query = {
            'more_like_this': {
                "fields": ["Keywords", "RecipeIngredientParts", "RecipeCategory"],
                "like": like, "min_term_freq": 1, "min_doc_freq": 5, "max_query_terms": 20,
            }
        }
    elif len(user['interestedCategory']) > 0:
        suggestion_query = {
            "match": {
                "Keywords": ' '.join(user['interestedCategory'])
            }
        }
    else:
        suggestion_query = {
            "function_score": {
                "query": {"match_all": {}},
                "random_score": {}
            }
        }

    suggestions = app.es_client.search(index='recipe', query=suggestion_query, size=suggest_size)
    suggestions_df = dataframe_parser(suggestions)

    end = time.time()
    response = {'status': 'success', 'results': result, 'elapse': end - start,
                'suggestions': suggestions_df.to_dict("records")}

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


@app.route('/recipe/correction', methods=["GET"])
def correction():
    start = time.time()
    text = request.args.get('text')

    suggest_dictionary = {
        "text": text,
        "autocomplete-1": {"term": {"field": "Description"}},
        "autocomplete-2": {"term": {"field": "Name"}},
        "autocomplete-3": {"term": {"field": "RecipeInstructions"}},
    }

    tokens = text.lower().split(' ')
    query_dictionary = {"suggest": suggest_dictionary}
    res = app.es_client.search(index="recipe", body=query_dictionary)
    resp = spell_correction_parser(np.array(list(res["suggest"].values())).T)

    result = []
    for i, token in enumerate(tokens):
        result.append(
            {
                "text": token,
                "candidates": resp[i]
            }
        )

    end = time.time()
    response = {'elapse': end - start, 'result': result, 'status': 'success'}

    return response


@app.route('/chat', methods=["GET"])
def chat():
    start = time.time()
    text = request.args.get('text')
    ai_resp = app.openai_client.chat.completions.create(
        model="gpt-3.5-turbo-0125",
        # response_format={"type": "json_object"},
        messages=[
            {"role": "system", "content": "You are cooking AI assistance."},
            {"role": "user", "content": text}
        ]
    )

    print(ai_resp.choices[0].message.content)
    end = time.time()

    response = {'elapse': end - start, "status": "success", "result": ai_resp.choices[0].message.content}
    return response


@app.route('/lang-chain', methods=["GET"])
def lang_chain():
    start = time.time()
    text = request.args.get('text')

    model_name = "gpt-3.5-turbo"
    llm = ChatOpenAI(model_name=model_name)
    chain = load_qa_chain(llm, chain_type="stuff", verbose=True)
    query = text
    matching_docs = app.vec_db.similarity_search(query)
    answer = chain.run(input_documents=matching_docs, question=query)
    end = time.time()

    response = {'elapse': end - start, "status": "success", "result": answer}
    return response

if __name__ == '__main__':
    app.run(debug=False)
