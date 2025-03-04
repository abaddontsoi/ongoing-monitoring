import asyncio
from bson import ObjectId
from dotenv import load_dotenv
from pymongo import AsyncMongoClient
import os
from utilities.fetch import fetch
from utilities.adverse_media_search import get_adverse_media
from datetime import datetime
from utilities.load_template import load_ongoing_template   
from utilities.judgment import get_judgments
from rapidfuzz import fuzz


async def cross_search_history_changelogs(history_data: list[dict], changelog_data: list[dict]) -> list[dict]:
    results = []

    similarity_cartesian_product = []
    for changelog in changelog_data:
        # Search whether changelog's target is in history_data
        target = changelog["new_data"].get("target", [])
        if isinstance(target, list) and len(target) > 0:
            # Calculate and store the similarity between changelog's target and history_data's nameEN, nameZH, nameEN + nameZH
            for history in history_data:
                target_en_list = [item.get("name_en", "").lower() for item in target]
                target_zh_list = [item.get("name_zh", "") for item in target]
                target_en_zh_list = [item.get("name_en", "") + " " + item.get("name_zh", "") for item in target]
                
                should_append = True
                nameEN, nameZH = history.get("nameEN", "").lower(), history.get("nameZH", "")
                
                # Check English name similarity
                if nameEN:
                    for target_en in target_en_list:
                        similarity_en = fuzz.ratio(target_en, nameEN)
                        if similarity_en >= 80:
                            should_append = True
                            break
                        should_append = False
                
                # If English name doesn't match, check Chinese name similarity
                if not should_append and nameZH:
                    for target_zh in target_zh_list:
                        similarity_zh = fuzz.ratio(target_zh, nameZH) 
                        if similarity_zh >= 80:
                            should_append = True
                            break
                        should_append = False
                
                # Check combined name similarity if individual matches failed
                if not should_append:
                    for target_en_zh in target_en_zh_list:
                        similarity_en_zh = fuzz.ratio(target_en_zh, nameEN + " " + nameZH)
                        if similarity_en_zh >= 80:
                            should_append = True
                            break
                        should_append = False
                
                if should_append:
                    similarity_cartesian_product.append((history, changelog))
                    

    # Sort similarity_cartesian_product by history's _id
    similarity_cartesian_product.sort(key=lambda x: x[0]["_id"], reverse=True)
    
    # Group by history's _id
    grouped_similarity_cartesian_product = {}
    for history, changelog in similarity_cartesian_product:
        if history["_id"] not in grouped_similarity_cartesian_product:
            grouped_similarity_cartesian_product[history["_id"]] = {
                "searchBy": history['searchBy'],
                "changelogs": []
            }
        grouped_similarity_cartesian_product[history["_id"]]["changelogs"].append(changelog)
        
    for k, v in grouped_similarity_cartesian_product.items():
        changelogs = []
        for changelog in v["changelogs"]:
            log = {}
            log["sourcedata_changelogs_id"] = changelog["_id"]
            log['data_id'] = changelog['original_data_id']
            log['type'] = changelog['action']
            log['category'] = changelog['category']
            if changelog['action'] == 'ADD':
                log['new_data'] = changelog['new_data']
            elif changelog['action'] == 'MOD':
                log['new_data'] = changelog['new_data']
                if changelog['changes']:
                    log['changes'] = changelog['changes']
                    
            log['createdAt'] = datetime.now()
            log['updatedAt'] = datetime.now()
            changelogs.append(log)
        
        result = await load_ongoing_template()
        result["aml_history_id"] = k
        result["searchBy"] = v["searchBy"]
        result["status"] = "todo"
        result["createdAt"] = datetime.now()
        result["updatedAt"] = datetime.now()
        result["data"] = changelogs
        results.append(result)

    return results

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
    aml_ongoing_monitoring_data = await cross_search_history_changelogs(history_data, changelog_data)
    
    await client[test_db_name][aml_ongoing_monitoring_collection_name].insert_many(aml_ongoing_monitoring_data)
    
    # Close connection
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
