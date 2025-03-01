from pymongo import AsyncMongoClient
from typing import List, Dict, Any
import os
import json
from utilities.text_similarity import get_similarities


async def get_adverse_media(collection, name: str) -> List[Dict]:
    
    if not name:
        return []
    
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
    
    cursor = await collection.aggregate(pipeline)
    
    result = []
    
    print(f"Starting cursor iteration for name: {name}")
    count = 0
    async for item in cursor:
        count += 1
        print(f"Processing item #{count}")
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

    print(f"Processed {count} items from cursor")
    print(f'After similarity filtering: {len(result)} records')
    
    formatted_data = []
    for item in result:
        print(f"Formatting item: {item['_id']}")
        subtitle = item['source']['title']
        if item.get('published'):
            subtitle += f" - {item['published']}"
            
        formatted_item = {
            '_id': str(item['_id']),
            'title': item.get('headline', {}).get('en'),
            'link': item['urls'][0] if item.get('urls') else None,
            'subtitle': subtitle,
            'description': item.get('content', {}).get('en'),
        }
        print(f"Formatted item: {json.dumps(formatted_item, indent=2)}")
        formatted_data.append(formatted_item)

    print(f"Final formatted data count: {len(formatted_data)}")
    return formatted_data

async def handler(client: AsyncMongoClient, search_name: list[str]) -> Dict[str, Any]:
    
    # Array of names to search
    name_to_search_arr = search_name

    try:
        db = client[os.getenv('SOURCE_DATABASE_NAME')]
        collection = db[os.getenv('MEDIA_COLLECTION_NAME')]

        data = []
        for name in name_to_search_arr:
            try:
                results = await get_adverse_media(collection, name)
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