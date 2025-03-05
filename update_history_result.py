from dotenv import load_dotenv
import os
from pymongo import AsyncMongoClient
import asyncio
from utilities.fetch import fetch
from datetime import datetime

async def find_history_result_by_data_id(history_results: list[dict], data_id: str|None):
    if not data_id:
        return None
    
    for history_result in history_results:
        if history_result.get("data_id") == data_id:
            return history_result
    return None

async def handle_group(collection, group: dict):
    history_result_tasks = []
    finished_ongoing_ids = []
    for history_id, data in group.items():
        ongoings = data.get("ongoing")
        history_results = data.get("history_result")
        if ongoings and history_results:
            changelog_data = []
            for ongoing in ongoings:
                changelog_data.extend(ongoing.get("data", []))
            
            for changelog in changelog_data:
                history_result = await find_history_result_by_data_id(history_results, changelog.get("data_id"))
                if history_result:
                    if changelog.get("type") == "MOD":
                        history_result_tasks.append(collection.update_one(
                            {"_id": history_result.get("_id")},
                            {"$set": {
                                "result": changelog.get("new_data"),
                                "updatedAt": datetime.now()
                            }}
                        ))
                else:
                    if changelog.get("type") == "ADD":
                        history_result_tasks.append(collection.insert_one(
                            {
                                "aml_history_id": history_id,
                                "type": changelog.get("category"),
                                "category": changelog.get("category"),
                                "data_id": str(changelog.get("data_id")),
                                "result": changelog.get("new_data"),
                                "createdAt": datetime.now(),
                                "updatedAt": datetime.now()
                            }
                        ))
        
        finished_ongoing_ids.extend([ongoing.get("_id") for ongoing in ongoings])
    
    await asyncio.gather(*history_result_tasks)
    return finished_ongoing_ids

async def main():
    load_dotenv()
    CONNECTION_STRING = str(os.getenv("CONNECTION_STRING"))
    client = AsyncMongoClient(CONNECTION_STRING)
    
    # source_db_name = str(os.getenv("SOURCE_DATABASE_NAME"))
    # media_collection_name = str(os.getenv("MEDIA_COLLECTION_NAME"))
    # judgment_collection_name = str(os.getenv("JUDGMENT_COLLECTION_NAME"))
    # sourcedata_changelogs_collection_name = str(os.getenv("SOURCEDATA_CHANGELOGS_COLLECTION_NAME"))
    
    test_db_name = str(os.getenv("TEST_DATABASE_NAME"))
    history_collection_name = str(os.getenv("HISTORY_COLLECTION_NAME"))
    history_result_collection_name = str(os.getenv("HISTORY_RESULT_COLLECTION_NAME"))
    aml_ongoing_monitoring_collection_name = str(os.getenv("AML_ONGOING_MONITORING_COLLECTION_NAME"))
    
    # Fetch ongoing records and sort data by createdAt
    ongoing = await fetch(client, test_db_name, aml_ongoing_monitoring_collection_name, {"status": "todo"})
    for item in ongoing:
        if isinstance(item.get("data"), list):
            data_list = item.get("data")
            data_list.sort(key=lambda x: x.get("createdAt"))
            item["data"] = data_list

    # Group by history_id
    grouped = {}
    for item in ongoing:
        history_id = item['aml_history_id']
        if history_id not in grouped:
            grouped[history_id] = {
                "ongoing": [],
                "history_result": []
            }
        grouped[history_id]["ongoing"].append(item)

    # Blob object id
    history_id = [item['aml_history_id'] for item in ongoing]
    
    # Fetch history results
    history_results = await fetch(client, test_db_name, history_result_collection_name, {"aml_history_id": {"$in": history_id}})
    for result in history_results: 
        history_id = result['aml_history_id']
        if history_id not in grouped:
            grouped[history_id] = {
                "ongoing": [],
                "history_result": []
            }
        grouped[history_id]["history_result"].append(result)
        
    finished_ongoing_ids = await handle_group(client[test_db_name][history_result_collection_name], grouped)
    await client[test_db_name][aml_ongoing_monitoring_collection_name].update_many(
        {"_id": {"$in": finished_ongoing_ids}},
        {"$set": {"status": "done"}}
    )

    
if __name__ == "__main__":
    asyncio.run(main())
