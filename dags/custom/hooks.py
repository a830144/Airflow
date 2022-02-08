from airflow.hooks.base_hook import BaseHook
class MovielensHook(BaseHook):

	DEFAULT_HOST = "movielens"
	DEFAULT_SCHEMA = "http"
	DEFAULT_PORT = 5000

	def __init__(self, conn_id):
		super().__init__() 
		self._conn_id = conn_id

	def get_conn(self):
		config = self.get_connection(self._conn_id)
		schema = config.schema or self.DEFAULT_SCHEMA
		host = config.host or self.DEFAULT_HOST
		port = config.port or self.DEFAULT_PORT
		base_url = f"{schema}://{host}:{port}"
		session = requests.Session()
		if config.login:
			session.auth = (config.login, config.password)
		return session, base_url