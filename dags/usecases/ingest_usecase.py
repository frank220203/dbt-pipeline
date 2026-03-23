from typing import List

import pandas as pd

from dbs.duck_db import Duck
from dbs.mongo_db import Mongo

class IngestUsecase:
    def __init__(self, mongo_uri: str):
        self.mongo = Mongo(mongo_uri)

    def ingest_mongo_to_duck(self, db_name: str, collections: List[str], **kwargs):
        
        for col_name in collections:
            collection = self.mongo.select_collection(db_name, col_name)
            data = list(collection.find({}, {"_id":0}))
            if not data:
                print("Collection has no data")
            else:
                df = pd.DataFrame(data)
                with Duck() as duck:
                    con = duck.get_con()
                    con.execute(f"CREATE OR REPLACE TABLE {col_name} AS SELECT * FROM df")
                    print(f"Loaded {len(df)} rows")