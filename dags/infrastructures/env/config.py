from airflow.models import Variable

class Config:
    project_id = Variable.get("project_id", default_var='dbt-pipeline')
    catchup = False
    svc_infos_id = ['table', 'svc_nm']

    def get_svc_infos(self):
        return Variable.get("svc_infos", deserialize_json=True)
    
    def get_mongo_uri(self):
        return Variable.get("mongo_uri")
    
    def get_duck_path(self):
        return f"{Variable.get("duck_path")}/{self.project_id}"