import pymongo

class Mongo:
    def __init__(self, mongo_uri: str):
        self.__mongo_client = pymongo.MongoClient(mongo_uri)

    def select_collection(self, db_name: str, col_name: str):
        mongo_db = self.__mongo_client[db_name]
        return mongo_db[col_name]