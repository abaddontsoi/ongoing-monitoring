from pymongo import MongoClient
from typing import List, Dict, Any
import os
import json
from utilities.text_similarity import get_similarities  # You'll need an equivalent Python package

DATASET_MONGO_URI = os.environ.get('DATASET_MONGO_URI')
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
        client = MongoClient(DATASET_MONGO_URI)
        cached_client = client
        cached_db = client[DATASET_DB_NAME]
        return cached_client
    except Exception as error:
        print(f'Failed to connect to MongoDB: {error}')
        raise Exception('Failed to connect to MongoDB')

async def get_adverse_media(db, name: str) -> List[Dict]:
    # Preprocess search name
    normalized_search_name = name.lower().strip()
    print(f'Original search name: {name}')
    print(f'Normalized search name: {normalized_search_name}')

    # Get candidates with loose conditions
    pipeline = [
        # Unwind target array
        {"$unwind": {"path": "$target", "preserveNullAndEmptyArrays": True}},
        
        # Match conditions
        {
            "$match": {
                "$or": [
                    {"target.name_en": {"$regex": f".*{normalized_search_name}.*", "$options": "i"}},
                    {"target.name_zh": {"$regex": f".*{normalized_search_name}.*", "$options": "i"}},
                    {"target.en.ceName": {"$regex": f".*{normalized_search_name}.*", "$options": "i"}},
                    {"target.zh.ceName": {"$regex": f".*{normalized_search_name}.*", "$options": "i"}},
                ]
            }
        },
        
        # Regroup documents with same _id
        {
            "$group": {
                "_id": "$_id",
                "doc": {"$first": "$$ROOT"}
            }
        },
        
        # Restore document structure
        {"$replaceRoot": {"newRoot": "$doc"}}
    ]

    print(f'Aggregation pipeline: {json.dumps(pipeline, indent=2)}')
    
    candidates = list(db.adverse_media.aggregate(pipeline))
    print(f'Found {len(candidates)} candidate records')

    # Filter using string similarity
    result = []
    for item in candidates:
        print(f'Checking item: {json.dumps(item.get("target"), indent=2)}')

        if item.get('target', {}).get('name_en'):
            print('Comparing strings:', {
                'search_name': normalized_search_name,
                'target_name': item['target']['name_en'].lower(),
            })
            
            similarities = get_similarities(
                normalized_search_name,
                [item['target']['name_en'].lower()],
                case_sensitive=False,
                order_sensitive=False,
                threshold=20,
                threshold_type='>='
            )
            
            print(f'Similarity results: {similarities}')
            
            if similarities:
                print(f'Found match! Similarity: {similarities[0]}')
                result.append(item)
                continue

        # Check second format
        if item.get('target', {}).get('en'):
            print(f'Checking second format: {json.dumps(item["target"]["en"], indent=2)}')

            for target in item['target']['en']:
                print('Comparing strings:', {
                    'search_name': normalized_search_name,
                    'target_name': target['ceName'].lower(),
                })
                
                similarities = get_similarities(
                    normalized_search_name,
                    [target['ceName'].lower()],
                    case_sensitive=False,
                    order_sensitive=False,
                    threshold=20,
                    threshold_type='>='
                )
                
                if similarities:
                    print(f'Found match! Similarity: {similarities[0]}')
                    result.append(item)
                    break

    print(f'After similarity filtering: {len(result)} records')
    
    formatted_data = []
    for item in result:
        subtitle = item['source']['title']
        if item.get('published'):
            subtitle += f" - {item['published']}"
            
        formatted_data.append({
            '_id': item['_id'],
            'title': item.get('headline', {}).get('en'),
            'link': item['urls'][0] if item.get('urls') else None,
            'subtitle': subtitle,
            'description': item.get('content', {}).get('en'),
        })

    return formatted_data

def handler(event: Dict[str, Any]) -> Dict[str, Any]:
    name_to_search_arr = event.get('nameToSearchArr')

    try:
        client = get_mongo_client()
        db = client[DATASET_DB_NAME]
        
        data = []
        for name in name_to_search_arr:
            try:
                results = get_adverse_media(db, name)
                data.extend(results)
            except Exception as error:
                print(f'Error searching for adverse media with name {name}: {error}')
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
        print(f'Adverse media search failed: {error}')
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Adverse media search failed',
                'error': str(error)
            })
        } 