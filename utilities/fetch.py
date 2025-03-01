from pymongo import AsyncMongoClient

async def fetch(client: AsyncMongoClient, database_name: str, collection_name: str, condition: dict):
    db = client[database_name]
    collection = db[collection_name]
    data = await collection.find(condition).to_list()
    return data
