import pandas as pd
from elastic_transport import ObjectApiResponse


def get_queries_from_user(user, boost_keyword=1, boost_up=0.2, boost_down=0.5):
    queries = []
    un_prefer = set(user['uninterestedCategory'])  # boost down
    prefer = set(user['interestedCategory'])  # boost up

    if len(user['interestedCategory']) > 0:
        queries.append(
            {

                "boosting": {
                    "positive": {
                        "match": {"Keywords": ' '.join(prefer)},
                    },
                    "negative": {
                        "match": {"Keywords": ' '.join(un_prefer)}
                    },
                    "negative_boost": 0.7
                }

            })

    else:
        queries.append({
            "function_score": {
                "query": {"match_all": {}},
                "random_score": {}
            }
        })

    if len(user['interestedRecipe']) > 0:
        like = []
        for recipe in user['interestedRecipe']:
            like.append({'_id': recipe})
        queries.append({
            'more_like_this': {
                "fields": ["Name", "Keywords", "RecipeIngredientParts", "RecipeCategory"],
                "like": like, "min_term_freq": 1, "min_doc_freq": 5, "max_query_terms": 20,
                'boost': 1 + boost_up
            }
        })

    query = {"dis_max": {
        "queries": queries
    }}

    return query


def get_paginated_response(results, url, total, start=0, limit=20):
    start = int(start)
    limit = int(limit)
    obj = {}
    obj['start'] = start
    obj['limit'] = limit

    if start == 0:
        obj['previous'] = ''
    else:
        start_copy = max(0, start - limit)
        limit_copy = start
        obj['previous'] = url + '&start=%d&limit=%d' % (start_copy, limit_copy)
        # make next url
    if start + limit > total:
        obj['next'] = ''
    else:
        start_copy = start + limit
        obj['next'] = url + '&start=%d&limit=%d' % (start_copy, limit)

    results_df = dataframe_parser(results)
    results_df = results_df[start:(start + limit)]

    obj['total'] = total
    obj['results'] = results_df.to_dict("records")

    return obj


def dataframe_parser(results: ObjectApiResponse) -> pd.DataFrame:
    results_df = pd.DataFrame(
        [
            [
                hit["_id"],
                hit["_score"],
                hit["_source"]['RecipeId'],
                hit["_source"]['Name'],
                hit["_source"]['AuthorId'],
                hit["_source"]['AuthorName'],
                hit["_source"]['CookTime'],
                hit["_source"]['PrepTime'],
                hit["_source"]['TotalTime'],
                hit["_source"]['DatePublished'],
                hit["_source"]['Description'],
                hit["_source"]['Images'],
                hit["_source"]['RecipeCategory'],
                hit["_source"]['Keywords'],
                hit["_source"]['RecipeIngredientQuantities'],
                hit["_source"]['RecipeIngredientParts'],
                hit["_source"]['AggregatedRating'],
                hit["_source"]['Calories'],
                hit["_source"]['FatContent'],
                hit["_source"]['SaturatedFatContent'],
                hit["_source"]['CholesterolContent'],
                hit["_source"]['SodiumContent'],
                hit["_source"]['CarbohydrateContent'],
                hit["_source"]['FiberContent'],
                hit["_source"]['SugarContent'],
                hit["_source"]['ProteinContent'],
                hit["_source"]['RecipeServings'],
                hit["_source"]['RecipeYield'],
                hit["_source"]['RecipeInstructions']
            ]
            for hit in results['hits']['hits']
        ],
        columns=[
            '_id',
            'score',
            'RecipeId',
            'Name',
            'AuthorId',
            'AuthorName',
            'CookTime',
            'PrepTime',
            'TotalTime',
            'DatePublished',
            'Description',
            'Images',
            'RecipeCategory',
            'Keywords',
            'RecipeIngredientQuantities',
            'RecipeIngredientParts',
            'AggregatedRating',
            'Calories',
            'FatContent',
            'SaturatedFatContent',
            'CholesterolContent',
            'SodiumContent',
            'CarbohydrateContent',
            'FiberContent',
            'SugarContent',
            'ProteinContent',
            'RecipeServings',
            'RecipeYield',
            'RecipeInstructions'
        ]
    )
    return results_df


# np.array(list(res["suggest"].values())).T
def spell_correction_parser(res):
    p = []
    for term in res:
        result = {"text": term[0]["text"]}
        options = [v["options"] for v in term]
        result["candidates"] = {}
        for option in options:
            candidates = {}
            if len(option) > 0:
                candidates["text"] = option[0]["text"]
                for candidate in option:
                    if candidate["text"] not in result["candidates"]:
                        result["candidates"][candidate["text"]] = {
                            "score": candidate["score"],
                            "freq": candidate["freq"],
                        }
                    else:
                        result["candidates"][candidate["text"]]["score"] = (result["candidates"][candidate["text"]]["score"] * result["candidates"][candidate["text"]]["freq"]+ candidate["score"] * candidate["freq"]) / (result["candidates"][candidate["text"]]["freq"] + candidate["freq"])
                        result["candidates"][candidate["text"]]["freq"] = (result["candidates"][candidate["text"]]["freq"] + candidate["freq"])
        p += [result["candidates"]]
    suggestions = []

    for suggestion in p:
        new_data = [{'text': key, 'score': value['score'], 'freq': value['freq']} for key, value in
                    suggestion.items()]
        suggestions.append(new_data)
    return suggestions
