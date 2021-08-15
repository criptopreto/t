from bson.objectid import ObjectId
from pymongo import MongoClient

conexion = MongoClient('127.0.0.1:27017',username="enj0nach0",password="Cort4unh4!.")
db = conexion.oscillator

def exists_item(collection: str, key: str, value: str)-> ObjectId:
    try:
        query = db[collection].find_one({key: value})
        if query is not None:
            return query["_id"]
        else:
            return None
    except Exception as e:
        print("Checking if Item Exists |", e)
        return False

def get_itemid(collection: str, key: str, value: str)-> ObjectId:
    try:
        query = db[collection].find_one({key: value})
        if query is not None:
            return query["_id"]
        else:
            return ObjectId()
    except Exception as e:
        print("Error Get Item ID | ", e)
        return ObjectId()
