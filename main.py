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

async def handle_history(media_collection, judgment_collection, changelogs_collection, history: dict):
    
    # Load ongoing template
    result = await load_ongoing_template()
    result["aml_history_id"] = history["_id"]
    result["searchBy"] = history["searchBy"]
    result["status"] = "todo"
    result["createdAt"] = datetime.now()
    result["updatedAt"] = datetime.now()
    result["data"] = []
    
    
    search_adverse_media_zh = await get_adverse_media(media_collection, history.get("nameZH", None))
    search_adverse_media_en = await get_adverse_media(media_collection, history.get("nameEN", None))
    
    search_judgment_zh = await get_judgments(judgment_collection, history.get("nameZH", None))
    search_judgment_en = await get_judgments(judgment_collection, history.get("nameEN", None))
    
    search_results = []
    search_results.extend(search_adverse_media_zh)
    search_results.extend(search_adverse_media_en)
    search_results.extend(search_judgment_zh)
    search_results.extend(search_judgment_en)
    
    changelog_data = []
    changelog_ids = []
    for search_result in search_results:
        logs = changelogs_collection.find({"$or": [
            {"original_data_id": search_result["_id"]},
            {"new_data": {k: v for k, v in search_result.items() if k != '_id'}},
        ]})
        for log in logs:
            data = {
                "sourcedata_changelogs_id": log["_id"],
                "data_id": log["original_data_id"],
                "category": log["category"],
                "type": log["action"],
            }
            if log["action"] == "MOD":
                data["changes"] = log["changes"]
            elif log["action"] == "ADD":
                data["new_data"] = log["new_data"]
            
            changelog_data.append(data)
            changelog_ids.append(log["_id"])
            
    result["data"] = changelog_data
    return result, changelog_ids

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
    tasks = []
    for history in history_data:
        nameEN, nameZH = history.get("nameEN"), history.get("nameZH")
        names = []
        if nameEN:  
            names.append(nameEN)
        if nameZH:
            names.append(nameZH)
        print(f"Searching for {names}")
        tasks.append(handle_history(client[source_db_name][media_collection_name], 
                                    client[source_db_name][judgment_collection_name],
                                    client[source_db_name][sourcedata_changelogs_collection_name],
                                    history))
        
    results = await asyncio.gather(*tasks)
    status_update_lists = [result[1] for result in results if result[0]['data']]
    status_update_ids = [item for sublist in status_update_lists for item in sublist]
    
    # Update status of changelogs to pending
    await client[source_db_name][sourcedata_changelogs_collection_name].update_many({"_id": {"$in": status_update_ids}}, {"$set": {"status": "completed"}})
    
    # Create ongoing monitoring record 
    for result in results:
        if result[0]['data']:
            await client[test_db_name][aml_ongoing_monitoring_collection_name].insert_one(result[0])
    
    # Close connection
    await client.close()

if __name__ == "__main__":
    asyncio.run(main())
