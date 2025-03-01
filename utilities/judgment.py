from pymongo import AsyncMongoClient
from typing import List, Dict, Any
import os
import json
from utilities.text_similarity import get_similarities  # You'll need an equivalent Python package

async def get_judgments(collection, name: str) -> List[Dict]:
    if not name:
        return []
    
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
    
    candidates = await collection.aggregate(pipeline)
    # print(f'Found {len(candidates)} candidate records')

    # Filter using string similarity
    result = []
    async for item in candidates:
        if isinstance(item.get('title'), str):
            lower_item_title = item['title'].lower()
            item["_id"] = str(item["_id"])
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

async def handler(client: AsyncMongoClient, search_name:list[str]) -> Dict[str, Any]:
    print(f'Received search_name: {search_name}')

    name_to_search_arr = search_name

    if not name_to_search_arr:
        return {
            'statusCode': 400,
            'body': json.dumps({
                'message': 'Missing required fields or invalid format: nameToSearchArr'
            })
        }

    try:
        db = client[os.getenv('SOURCE_DATABASE_NAME')]
        collection = db[os.getenv('JUDGMENT_COLLECTION_NAME')]

        data = []
        for name in name_to_search_arr:
            try:
                results = await get_judgments(collection, name)
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