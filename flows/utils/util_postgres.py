from sqlalchemy import create_engine
import urllib


class PostGres:

    def __init__(self, host, user, password, data_base):
        self.host = host
        self.user = user
        self.password = password
        self.data_base = data_base

    
    def engine(self):

        parsed_password = urllib.parse.quote_plus(self.password)
        
        connection_string = (
            f'''postgresql+psycopg2://{self.user}:{parsed_password}@{self.host}/{self.data_base}'''
        )

        engine = create_engine(
            connection_string
        )

        return engine