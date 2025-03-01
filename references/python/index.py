from pymongo import MongoClient
from typing import List, Dict, Any
import os
import json
from utilities.text_similarity import get_similarities  # You'll need an equivalent Python package

if not os.environ.get('MONGODB_URI'):
    raise ValueError('Invalid environment variable: "MONGODB_URI"')

MONGODB_URI = os.environ.get('MONGODB_URI')
DATASET_DB_NAME = os.environ.get('DATASET_DB_NAME')

cached_client = None
cached_db = None

def get_mongo_client():
    global cached_client, cached_db
    
    if cached_client and cached_db:
        try:
            cached_db.command('ping')
            return cached_client
        except Exception:
            cached_client.close()
            cached_client = None
            cached_db = None
    
    try:
        client = MongoClient(MONGODB_URI)
        cached_client = client
        cached_db = client[DATASET_DB_NAME]
        return cached_client
    except Exception as error:
        print(f'Failed to connect to MongoDB: {error}')
        raise Exception('Failed to connect to MongoDB')

async def get_judgments(db, name: str) -> List[Dict]:
    # Preprocess search name
    normalized_search_name = name.lower().strip()
    print(f'Original search name: {name}')
    print(f'Normalized search name: {normalized_search_name}')

    # Get candidates with loose conditions
    pipeline = [
        {
            '$match': {
                'title': {'$regex': f'.*{normalized_search_name}.*', '$options': 'i'}
            }
        }
    ]

    print(f'Aggregation pipeline: {json.dumps(pipeline, indent=2)}')
    
    candidates = list(await db.judgment.aggregate(pipeline))
    # print(f'Found {len(candidates)} candidate records')

    # Filter using string similarity
    result = []
    for item in candidates:
        if isinstance(item.get('title'), str):
            lower_item_title = item['title'].lower()
            print('Comparing strings:', {
                'search_name': normalized_search_name,
                'target_name': lower_item_title
            })

            similarities = get_similarities(
                normalized_search_name,
                [lower_item_title],
                case_sensitive=False,
                order_sensitive=False,
                threshold=20,
                threshold_type='>='
            )

            print(f'Similarity results: {similarities}')

            if similarities:
                print(f'Found match! Similarity: {similarities[0]}')
                result.append(item)

    print(f'After similarity filtering: {len(result)} records')
    
    if result:
        print(f'First record: {json.dumps(result[0], indent=2)}')

    return result

def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    print(f'Received event: {json.dumps(event, indent=2)}')

    name_to_search_arr = event.get('nameToSearchArr')

    if not name_to_search_arr:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 'Missing required fields or invalid format: nameToSearchArr'
            })
        }

    try:
        client = get_mongo_client()
        db = client[DATASET_DB_NAME]

        data = []
        for name in name_to_search_arr:
            try:
                results = get_judgments(db, name)
                data.extend(results)
            except Exception as error:
                print(f'Error searching for judgments with name {name}: {error}')
                continue

        return {
            'statusCode': 200,
            'body': json.dumps({
                'status': 200,
                'message': 'Success',
                'data': data
            })
        }
    except Exception as error:
        print(f'Judgment search failed: {error}')
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Judgment search failed',
                'error': str(error)
            })
        } 