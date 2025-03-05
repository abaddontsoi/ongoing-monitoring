import asyncio
from dotenv import load_dotenv
from pymongo import AsyncMongoClient
import os
from utilities.fetch import fetch
from datetime import datetime
from utilities.load_template import load_ongoing_template   
from rapidfuzz import fuzz
import re


async def cross_search_history_changelogs(history_data: list[dict], changelog_data: list[dict]) -> tuple[list[dict], list]:
    results = []
    
    cartesian_product = []
    for history in history_data: 
        nameEN = history.get("nameEN", "").lower()
        nameZH = history.get("nameZH", "")
        for changelog in changelog_data:
            if changelog.get("category") == "adverse media":
                if isinstance(changelog.get("new_data", {}).get("target"), list):
                    name_en_list = [item.get("name_en", "").lower() for item in changelog.get("new_data", {}).get("target", [])]
                    name_zh_list = [item.get("name_zh", "") for item in changelog.get("new_data", {}).get("target", [])]
                    match_name_en_list = []
                    match_name_zh_list = []
                    re_en_exp = re.compile(f".*{nameEN}.*")
                    re_zh_exp = re.compile(f".*{nameZH}.*")
                    for name_en in name_en_list:
                        print(f"Comparing {name_en} with {nameEN}")
                        if re.search(re_en_exp, name_en) and len(name_en) > 0:
                            if fuzz.ratio(name_en, nameEN) > 20:
                                match_name_en_list.append(name_en)
                            
                    for name_zh in name_zh_list:
                        print(f"Comparing {name_zh} with {nameZH}")
                        if re.search(re_zh_exp, name_zh) and len(name_zh) > 0:
                            if fuzz.ratio(name_zh, nameZH) > 20:
                                match_name_zh_list.append(name_zh)
                                
                    if match_name_en_list or match_name_zh_list:
                        cartesian_product.append((history, changelog)) 
                elif isinstance(changelog.get("new_data", {}).get("target"), dict):
                    en = changelog.get("new_data", {}).get("target", {}).get("en", {})
                    zh = changelog.get("new_data", {}).get("target", {}).get("zh", {})
                    
                    name_en_list = [item.get("ceName", "").lower() for item in en]
                    name_zh_list = [item.get("ceName", "") for item in zh]     
                    match_name_en_list = []
                    match_name_zh_list = []
                    re_en_exp = re.compile(f".*{nameEN}.*")
                    re_zh_exp = re.compile(f".*{nameZH}.*")
                    for name_en in name_en_list:
                        print(f"Comparing {name_en} with {nameEN}")
                        if re.search(re_en_exp, name_en) and len(name_en) > 0:
                            if fuzz.ratio(name_en, nameEN) > 20:
                                match_name_en_list.append(name_en)
                            
                    for name_zh in name_zh_list:
                        print(f"Comparing {name_zh} with {nameZH}")
                        if re.search(re_zh_exp, name_zh) and len(name_zh) > 0:
                            if fuzz.ratio(name_zh, nameZH) > 20:
                                match_name_zh_list.append(name_zh)
                                
                    if match_name_en_list or match_name_zh_list:
                        cartesian_product.append((history, changelog))
                        
            elif changelog.get("category") == "judgment":
                if isinstance(changelog.get("new_data"), dict):
                    title = changelog.get("new_data").get("title").lower()
                    if title:
                        re_en_exp = re.compile(f".*{nameEN}.*")
                        re_zh_exp = re.compile(f".*{nameZH}.*")
                        if re.search(re_en_exp, title) and len(title) > 0 and len(nameEN) > 0:
                            if fuzz.ratio(title, nameEN) > 20 or fuzz.ratio(title, nameZH) > 20:
                                cartesian_product.append((history, changelog))
                        elif re.search(re_zh_exp, title) and len(title) > 0 and len(nameZH) > 0:
                            if fuzz.ratio(title, nameZH) > 20:
                                cartesian_product.append((history, changelog))

                
    cartesian_product.sort(key=lambda x: x[0]['_id'])
    grouped = {}
    for history, changelog in cartesian_product:
        if history['_id'] not in grouped:
            grouped[history['_id']] = {
                'searchBy': history['searchBy'],
                'changelogs': []
            }
        grouped[history['_id']]['changelogs'].append(changelog)

    for history_id, data in grouped.items():
        formatted = []
        changelogs = data['changelogs']
        for changelog in changelogs:
            new_data = {
                "sourcedata_changelogs_id": changelog['_id'],
                "data_id": changelog.get("original_data_id", ""),
                'type': changelog['action'],
                "category": changelog['category'],
                "createdAt": datetime.now(),
                "updatedAt": datetime.now(),
                "new_data": changelog['new_data'],
                "old_data": changelog['old_data']
            }
                
            if changelog['action'] == 'MOD':
                new_data['changes'] = changelog['changes']
            
            formatted.append(new_data)
        
        result = await load_ongoing_template()
        result['aml_history_id'] = history_id
        result['searchBy'] = data['searchBy']
        result['data'] = formatted
        result['createdAt'] = datetime.now()
        result['updatedAt'] = datetime.now()
        results.append(result)
        
    return results, [product[1] for product in cartesian_product]

async def main():
    load_dotenv()
    CONNECTION_STRING = str(os.getenv("CONNECTION_STRING"))
    client = AsyncMongoClient(CONNECTION_STRING)
    
    source_db_name = str(os.getenv("SOURCE_DATABASE_NAME"))
    media_collection_name = str(os.getenv("MEDIA_COLLECTION_NAME"))
    judgment_collection_name = str(os.getenv("JUDGMENT_COLLECTION_NAME"))
    sourcedata_changelogs_collection_name = str(os.getenv("SOURCEDATA_CHANGELOGS_COLLECTION_NAME"))
    
    
    test_db_name = str(os.getenv("TEST_DATABASE_NAME"))
    history_collection_name = str(os.getenv("HISTORY_COLLECTION_NAME"))
    history_result_collection_name = str(os.getenv("HISTORY_RESULT_COLLECTION_NAME"))
    aml_ongoing_monitoring_collection_name = str(os.getenv("AML_ONGOING_MONITORING_COLLECTION_NAME"))
    
    # Get data from AML_history by filter ongoing_monitoring = true
    history_data = await fetch(client, test_db_name, history_collection_name, {"ongoing_monitoring": True})
    
    # Get changelogs by filter status = pending
    changelog_data = await fetch(client, source_db_name, sourcedata_changelogs_collection_name, {"status": "pending"})
    
    # Result of cross search
    aml_ongoing_monitoring_data, cartesian_product = await cross_search_history_changelogs(history_data, changelog_data)
    
    if aml_ongoing_monitoring_data:
        await client[test_db_name][aml_ongoing_monitoring_collection_name].insert_many(aml_ongoing_monitoring_data)
    
    if cartesian_product:
        # Update changelog status to completed
        await client[source_db_name][sourcedata_changelogs_collection_name].update_many(
            {"_id": {"$in": [product['_id'] for product in cartesian_product]}},
            {"$set": {"status": "completed"}}
        )
    
    # Close connection
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
