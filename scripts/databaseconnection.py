from sqlalchemy import create_engine
import pandas as pd

class MySqlConnect:

    def __init__(self, username='root', password='root', host='localhost', database='dm'):
        self.username = username
        self.password = password
        self.host = host
        self.database = database

    def connect(self):
        db_connection_str = 'mysql://'+self.username+':'+self.password+'@'+self.host+'/'+self.database
        return create_engine(db_connection_str)



if __name__ == '__main__':

    db = MySqlConnect()
    db_connection = db.connect()

    address_dim = pd.read_sql('SELECT * FROM address_dim where scd_end is null', con=db_connection)
    customer_dim = pd.read_sql('SELECT * FROM customer_dim where scd_end is null', con=db_connection)  #
    incidents_dim = pd.read_sql('SELECT * FROM incidents_dim where scd_end is null', con=db_connection)
    policy_csl_dim = pd.read_sql('SELECT * FROM policy_csl_dim where scd_end is null', con=db_connection)
    policy_fact = pd.read_sql('SELECT * FROM policy_fact', con=db_connection)
    policy_state_dim = pd.read_sql('SELECT * FROM policy_state_dim where scd_end is null', con=db_connection)
