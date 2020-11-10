from scripts.databaseconnection import MySqlConnect
from scripts.datapreprocessing.extraction import Extract
from scripts.datapreprocessing.preprocessing import Preprocessing
from scripts.datapreprocessing.model import Model

# Database Connection
db = MySqlConnect()
db_connection = db.connect()

# Filepath for Model/LabelEncoder
filepath = "C:/Users/shubh/OneDrive - SRH IT/University/Semester 3/Data Management 2/Project/Output"

# Extract Data from the Database
extract_obj = Extract(db_connection)
address_dim = extract_obj.address_dim()
customer_dim = extract_obj.customer_dim()
policy_csl_dim = extract_obj.policy_csl_dim()
incidents_dim = extract_obj.incidents_dim()
policy_fact = extract_obj.policy_fact()
policy_state_dim = extract_obj.policy_state_dim()
contact_dim = extract_obj.contact_dim()

# Merging Tables for Flat Records
prep = Preprocessing(customer_dim, address_dim, contact_dim, policy_csl_dim, incidents_dim, policy_fact,
                     policy_state_dim, filepath)
final_df = prep.merge_tables()
X_predict = final_df[:1:].copy()
# Drop SCD Columns Out
# final_df = prep.drop_scd_columns(final_df)

# Generate X
X = prep.generate_X(final_df)

# Change Strings to Categories and Label using Encoder
X = prep.encode_categories(X)

# Generate Y
Y = prep.generate_Y(final_df)

# Model Training
model = Model(X, Y, filepath)
model.DTC()
model.SVM()
model.RFC()
model.ABC()


# X Get Predictors
X_predict = Preprocessing.generate_prediction_X(X_predict)
X_predict = prep.encode_predict_records(X_predict)

# Load Models

# ["SVM","DecisionTreeClassifier", "RandomForestClassifier", "AdaBoostClassifier"]
clf, clf_dt, clf_rf, clf_ab = model.load_models()


print(clf.predict(X_predict))
print(clf_dt.predict(X_predict))
print(clf_rf.predict(X_predict))
print(clf_ab.predict(X_predict))







