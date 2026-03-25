from typing import List

import pandas as pd

from infrastructures.dbs.duck_db import Duck
from infrastructures.dbs.mongo_db import Mongo
from infrastructures.env.config import Config

class IngestUsecase:

    def __init__(self):
        self.config = Config()

    def create_dataset_table(self, svc_infos: List[str], duck_path: str):

        for svc_info in svc_infos:
            svc_nm = svc_info[self.config.svc_infos_id.index('svc_nm')]

            with Duck(duck_path) as duck:
                duck.create_dataset(svc_nm)
                

    def mig_mongo_to_duck(self, svc_infos: List[str], mongo_uri: str, duck_path: str, **kwargs):
        
        mongo = Mongo(mongo_uri)

        for svc_info in svc_infos:
            svc_nm = svc_info[self.config.svc_infos_id.index('svc_nm')]
            table = svc_info[self.config.svc_infos_id.index('table')]
            collection = mongo.select_collection("pipeline", table)
            data = list(collection.find({}, {"_id":0}))
            if not data:
                print("Collection has no data")
            else:
                df = pd.DataFrame(data)
                with Duck(duck_path) as duck:
                    con = duck.get_con()
                    con.execute(f"CREATE OR REPLACE TABLE {svc_nm}.{table} AS SELECT * FROM df")
                    print(f"Loaded {len(df)} rows")