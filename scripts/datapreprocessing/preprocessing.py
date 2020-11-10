from scripts.databaseconnection import MySqlConnect
from scripts.datapreprocessing.extraction import Extract
from sklearn.preprocessing import LabelEncoder
import pickle

class Preprocessing:

    def __init__(self, customer_dim, address_dim, contact_dim, policy_csl_dim, incidents_dim, policy_fact,
                 policy_state_dim, path):
        self.customer_dim = customer_dim
        self.address_dim = address_dim
        self.contact_dim = contact_dim
        self.policy_csl_dim = policy_csl_dim
        self.incidents_dim = incidents_dim
        self.policy_fact = policy_fact
        self.policy_state_dim = policy_state_dim
        self.path = path

    def merge_tables(self):
        final_df = self.policy_fact.merge(self.customer_dim, on="cust_id", how="left", suffixes=["_fact","_cust"])
        final_df = final_df.merge(self.address_dim, on="address_id", how="left", suffixes=["_fact", "_add"])
        final_df = final_df.merge(self.incidents_dim, on="incident_id", how="left", suffixes=["_fact","_inc"])
        final_df = final_df.merge(self.policy_csl_dim, on="policy_csl_id", how="left", suffixes=["_fact","_csl"])
        final_df = final_df.merge(self.policy_state_dim, on="policy_state_id", how="left", suffixes=["_fact","_state"])
        final_df = final_df.merge(self.contact_dim, on="contact_id", how="left", suffixes=["_fact","_cont"])
        return final_df

    def drop_scd_columns(self, df):
        # print(df.columns)
        df = df.drop(columns=['sk_fact', 'scd_start_fact', 'scd_end_fact', 'scd_version_fact',
                              'sk_add',
                              'scd_start_add', 'scd_end_add', 'sk_cont',
                              'scd_start_cont', 'scd_end_cont',
                              'scd_version_cont' ],axis = 1)
        # print(df.columns)
        return df

    def generate_Y(self, df):
        y = df.filter(items=['fraud_reported'])
        return y

    def generate_X(self, df):
        df = df.filter(items=['number_of_vehicles_involved',
                              'bodily_injuries', 'witnesses', 'total_claim_amount', 'injury_claim',
                              'property_claim', 'vehicle_claim', 'months_as_customer',
                              'policy_deductable', 'policy_annual_premium', 'umbrella_limit',
                              'capital_gains', 'capital_loss', 'age', 'insured_zip', 'insured_sex',
                              'insured_education_level', 'insured_occupation', 'insured_hobbies',
                              'insured_relationship', 'city_state', 'zip',
                              'incident_type', 'incident_severity',
                              'authorities_contacted', 'incident_state', 'incident_city',
                              'incident_hour_of_the_day', 'property_damage',
                              'police_report_available', 'collision_type',
                              'policy_csl', 'policy_state'])
        return df

    @staticmethod
    def generate_prediction_X(predict_value):
        predict_value = predict_value.filter(items=['number_of_vehicles_involved',
                                                    'bodily_injuries', 'witnesses', 'total_claim_amount',
                                                    'injury_claim',
                                                    'property_claim', 'vehicle_claim', 'months_as_customer',
                                                    'policy_deductable', 'policy_annual_premium', 'umbrella_limit',
                                                    'capital_gains', 'capital_loss', 'age', 'insured_zip',
                                                    'insured_sex',
                                                    'insured_education_level', 'insured_occupation', 'insured_hobbies',
                                                    'insured_relationship', 'city_state', 'zip',
                                                    'incident_type', 'incident_severity',
                                                    'authorities_contacted', 'incident_state', 'incident_city',
                                                    'incident_hour_of_the_day', 'property_damage',
                                                    'police_report_available', 'collision_type',
                                                    'policy_csl', 'policy_state'])
        return predict_value

    def encode_categories(self, df):
        df['incident_hour_of_the_day'] = df['incident_hour_of_the_day'].astype(int)
        df_type = df.copy()

        for eachCol in ['policy_deductable','insured_sex','insured_zip','insured_education_level', 'insured_occupation',
                        'insured_hobbies','insured_relationship','city_state','zip','incident_type', 'incident_severity'
            ,'authorities_contacted','incident_state',  'incident_city', 'property_damage','police_report_available',
                        'collision_type','policy_csl','policy_state']:
            df[eachCol] = df_type[eachCol].astype('category')

            label_encoder = LabelEncoder()
            # print(df[eachCol].dtypes)
            df[eachCol] = label_encoder.fit_transform(df[eachCol])
            with open(self.path + "/LabelEncoders/"+eachCol+'.pkl', 'wb') as output:
                pickle.dump(label_encoder, output, pickle.HIGHEST_PROTOCOL)

        # print(df.dtypes)
        return df

    def encode_predict_records(self, df):
        for eachCol in ['policy_deductable', 'insured_sex', 'insured_zip', 'insured_education_level',
                        'insured_occupation', 'insured_hobbies',
                        'insured_relationship', 'city_state', 'zip', 'incident_type', 'incident_severity',
                        'authorities_contacted', 'incident_state',
                        'incident_city', 'property_damage', 'police_report_available', 'collision_type', 'policy_csl',
                        'policy_state']:
            # print(eachCol)
            df[eachCol] = df[eachCol].astype('category')
            with open(self.path + "/LabelEncoders/"+eachCol+'.pkl',"rb") as input_file:
                e = pickle.load(input_file)
                df[eachCol] = e.transform(df[eachCol])
        return df

if __name__ == '__main__':
    db = MySqlConnect()
    db_connection = db.connect()
    extract_obj = Extract(db_connection)
    address_dim = extract_obj.address_dim()
    customer_dim = extract_obj.customer_dim()
    policy_csl_dim = extract_obj.policy_csl_dim()
    incidents_dim = extract_obj.incidents_dim()
    contact_dim = extract_obj.contact_dim()
    policy_fact = extract_obj.policy_fact()
    policy_state_dim = extract_obj.policy_state_dim()
    path = "C:/Users/shubh/OneDrive - SRH IT/University/Semester 3/Data Management 2/Project/Output"
    prep = Preprocessing(customer_dim,address_dim,contact_dim,policy_csl_dim,incidents_dim,policy_fact,
                         policy_state_dim, path)
    final_df = prep.merge_tables()
    final_df = prep.drop_scd_columns(final_df)
    X = prep.generate_X(final_df)
    X = prep.encode_categories(X)
    Y = prep.generate_Y(final_df)
    # print(X.dtypes)

