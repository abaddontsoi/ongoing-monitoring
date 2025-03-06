from dotenv import load_dotenv
import os
from pymongo import AsyncMongoClient
import asyncio
from datetime import datetime, timedelta

async def main():
    load_dotenv()
    CONNECTION_STRING = str(os.getenv("CONNECTION_STRING"))
    client = AsyncMongoClient(CONNECTION_STRING)
    
    source_db_name = str(os.getenv("SOURCE_DATABASE_NAME"))
    test_db_name = str(os.getenv("TEST_DATABASE_NAME"))
    sourcedata_changelogs_collection_name = str(os.getenv("SOURCEDATA_CHANGELOGS_COLLECTION_NAME"))
    history_result_collection_name = str(os.getenv("HISTORY_RESULT_COLLECTION_NAME"))   
    history_result_collection = client[test_db_name][history_result_collection_name]

    aml_ongoing_monitoring_collection_name = str(os.getenv("AML_ONGOING_MONITORING_COLLECTION_NAME"))
    aml_ongoing_monitoring_collection = client[test_db_name][aml_ongoing_monitoring_collection_name]
    sourcedata_changelogs_collection = client[source_db_name][sourcedata_changelogs_collection_name]

    # Reset sourcedata_changelogs
    await sourcedata_changelogs_collection.update_many({}, {
        "$set": {
            "status": "pending"
        }
    })

    # Reset aml_ongoing_monitoring
    await aml_ongoing_monitoring_collection.delete_many({
    })
    
    # Reset history_result
    await history_result_collection.delete_many({
        "createdAt": {"$gte": datetime.now() - timedelta(days=1)}
    })

if __name__ == "__main__":
    asyncio.run(main())
