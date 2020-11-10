import pandas as pd
from scripts.databaseconnection import MySqlConnect


class Extract:

    def __init__(self, connection):
        self.connection = connection

    def address_dim(self):
        # address_dim = pd.read_sql('SELECT * FROM address_dim where scd_end is null', con=self.connection)
        address_dim = pd.read_sql('SELECT address_id, street_address, city_state, zip FROM dm.address_dim'
                                  ' where scd_end is null', con=self.connection)
        return address_dim

    def customer_dim(self):
        customer_dim = pd.read_sql('SELECT cust_id, address_id, contact_id, first_name, last_name, age, insured_zip, '
                                   'insured_sex, insured_education_level, insured_occupation, insured_hobbies, '
                                   'insured_relationship FROM dm.customer_dim where scd_end is null'
                                   , con=self.connection) #
        # customer_dim = pd.read_sql('SELECT * FROM customer_dim where scd_end is null', con=self.connection) #
        return customer_dim

    def incidents_dim(self):
        incidents_dim = pd.read_sql('SELECT incident_id, incident_date, incident_type, incident_severity, '
                                    'authorities_contacted, incident_state, incident_city, incident_location, '
                                    'incident_hour_of_the_day, property_damage, police_report_available, '
                                    'fraud_reported, collision_type FROM dm.incidents_dim where scd_end is null',
                                    con=self.connection)
        # incidents_dim = pd.read_sql('SELECT * FROM incidents_dim where scd_end is null', con=self.connection)
        return incidents_dim

    def policy_csl_dim(self):
        # policy_csl_dim = pd.read_sql('SELECT * FROM policy_csl_dim where scd_end is null', con=self.connection)
        policy_csl_dim = pd.read_sql('SELECT policy_csl_id, policy_csl FROM dm.policy_csl_dim'
                                     ' where scd_end is null', con=self.connection)
        return policy_csl_dim

    def policy_fact(self):
        policy_fact = pd.read_sql('SELECT policy_number, incident_id, number_of_vehicles_involved, bodily_injuries, '
                                  'witnesses, total_claim_amount, injury_claim, property_claim, vehicle_claim, '
                                  'months_as_customer, policy_bind_date, policy_csl_id, policy_state_id, '
                                  'policy_deductable, policy_annual_premium, umbrella_limit, capital_gains, capital_loss, '
                                  'cust_id FROM dm.policy_fact', con=self.connection)
        # policy_fact = pd.read_sql('SELECT * FROM policy_fact', con=self.connection)
        return policy_fact

    def policy_state_dim(self):
        policy_state_dim = pd.read_sql('SELECT policy_state_id, policy_state FROM dm.policy_state_dim'
                                       ' where scd_end is null', con=self.connection)
        # policy_state_dim = pd.read_sql('SELECT * FROM policy_state_dim where scd_end is null', con=self.connection)
        return  policy_state_dim

    def contact_dim(self):
        # contact_dim = pd.read_sql('SELECT * FROM contact_dim where scd_end is null', con=self.connection)
        contact_dim = pd.read_sql('SELECT contact_id, email, personal_number, company_number FROM dm.contact_dim'
                                  ' where scd_end is null', con=self.connection)
        return  contact_dim



if __name__ == '__main__':
    db = MySqlConnect()
    db_connection = db.connect()
    extract_obj = Extract(db_connection)
    address_dim = extract_obj.address_dim()
    contact_dim = extract_obj.contact_dim()
    customer_dim = extract_obj.customer_dim()
    policy_csl_dim = extract_obj.policy_csl_dim()
    incidents_dim = extract_obj.incidents_dim()
    policy_fact = extract_obj.policy_fact()
    policy_state_dim = extract_obj.policy_state_dim()


