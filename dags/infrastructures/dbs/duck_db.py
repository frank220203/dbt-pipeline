import duckdb

class Duck:
    def __init__(self, duck_path: str):
        self.db_path = f"{duck_path}.duckdb"
    
    def __enter__(self):
        self.con = duckdb.connect(self.db_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.con:
            self.con.close()

    def get_con(self):
        return self.con
    
    def create_dataset(self, svc_nm: str):
        self.con.execute(f"CREATE SCHEMA IF NOT EXISTS {svc_nm}")