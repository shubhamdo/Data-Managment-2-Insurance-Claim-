import pandas as pd
from scripts.databaseconnection import MySqlConnect

db_connection = MySqlConnect()

class Extract:

    def __init__(self, connection):
        self.connection = connection

    def address_dim(self):
        address_dim = pd.read_sql('SELECT * FROM address_dim where scd_end is null', con=self.connection)
        return address_dim

    def customer_dim(self):
        customer_dim = pd.read_sql('SELECT * FROM customer_dim where scd_end is null', con=self.connection) #
        return customer_dim

    def incidents_dim(self):
        incidents_dim = pd.read_sql('SELECT * FROM incidents_dim where scd_end is null', con=self.connection)
        return incidents_dim

    def policy_csl_dim(self):
        policy_csl_dim = pd.read_sql('SELECT * FROM policy_csl_dim where scd_end is null', con=self.connection)
        return policy_csl_dim

    def policy_fact(self):
        policy_fact = pd.read_sql('SELECT * FROM policy_fact', con=self.connection)
        return policy_fact

    def policy_state_dim(self):
        policy_state_dim = pd.read_sql('SELECT * FROM policy_state_dim where scd_end is null', con=self.connection)
        return  policy_state_dim

