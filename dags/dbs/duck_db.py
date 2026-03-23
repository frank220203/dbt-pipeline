import duckdb

class Duck:
    def __init__(self):
        self.db_path = "/opt/airflow/duckdb/local.duckdb"
    
    def __enter__(self):
        self.con = duckdb.connect(self.db_path)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.con:
            self.con.close()

    def get_con(self):
        return self.con