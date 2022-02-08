from airflow.models.baseoperator import BaseOperator
from airflow.hooks.mysql_hook import MySqlHook


class HelloDBOperator(BaseOperator):
    def __init__(self, name: str, mysql_conn: str, database: str, **kwargs) -> None:
        super().__init__(**kwargs)
        self.name = name
        self.mysql_conn = mysql_conn
        self.database = database

    def execute(self, context):
        hook = MySqlHook(mysql_conn_id=self.mysql_conn, schema=self.database)
        sql = "select role_name from roles"
        result = hook.get_first(sql)
        message = f"Hello {result[0]}"
        print(message)
        return message