from scripts.databaseconnection import MySqlConnect
from scripts.datapreprocessing.extraction import Extract
from scripts.datapreprocessing.preprocessing import Preprocessing
from sklearn.model_selection import train_test_split
from sklearn.linear_model import SGDClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier
import pickle

class Model:


    def __init__(self, x, y, path):
        self.x = x
        self.y = y
        self.path = path
        self.X_train, self.X_test, self.Y_train, self.Y_test = train_test_split(self.x, self.y, test_size=0.3, random_state=42)

    # SVM

    def SVM(self):

        clf_svm = SGDClassifier(n_jobs=6, loss='hinge', penalty='l2', alpha=1000, random_state=69, max_iter=1000, tol=None)
        clf_svm.fit(self.X_train, self.Y_train.values.ravel())
        score_svm = clf_svm.score(self.X_test, self.Y_test)
        print("SVM", score_svm)
        with open(
                self.path + "/Models/" + "SVM" + '.pkl',
                'wb') as output:
            pickle.dump(clf_svm, output, pickle.HIGHEST_PROTOCOL)

    # DecisionTreeClassifier
    def DTC(self):
        clf_dt = DecisionTreeClassifier(max_depth=5)
        clf_dt.fit(self.X_train, self.Y_train.values.ravel())
        score_dt = clf_dt.score(self.X_test, self.Y_test)
        print("Decison Tree", score_dt)
        with open(
                self.path + "/Models/" + "DecisionTreeClassifier" + '.pkl',
                'wb') as output:
            pickle.dump(clf_dt, output, pickle.HIGHEST_PROTOCOL)

    def RFC(self):
        clf_rf = RandomForestClassifier(max_depth=5, n_estimators=100)
        clf_rf.fit(self.X_train, self.Y_train.values.ravel())
        score_rf = clf_rf.score(self.X_test,self.Y_test)
        print("Random Forest", score_rf)
        with open(
                self.path + "/Models/" + "RandomForestClassifier" + '.pkl',
                'wb') as output:
            pickle.dump(clf_rf, output, pickle.HIGHEST_PROTOCOL)

    def ABC(self):
        clf_ab = AdaBoostClassifier(n_estimators=100)
        clf_ab.fit(self.X_train, self.Y_train.values.ravel())
        score_ab = clf_ab.score(self.X_test, self.Y_test)
        print("Ada Boost", score_ab)
        with open(
                self.path + "/Models/" + "AdaBoostClassifier" + '.pkl',
                'wb') as output:
            pickle.dump(clf_ab, output, pickle.HIGHEST_PROTOCOL)

    def load_models(self):
        models = ["SVM","DecisionTreeClassifier", "RandomForestClassifier", "AdaBoostClassifier"]
        modelsList = []
        for eachModel in models:
            with open(self.path + "/Models/" + eachModel + '.pkl', "rb") as input_file:
                e = pickle.load(input_file)
                modelsList.append(e)
        return modelsList

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
    prep = Preprocessing(customer_dim, address_dim,contact_dim,policy_csl_dim,incidents_dim,policy_fact,policy_state_dim)
    final_df = prep.merge_tables()
    final_df = prep.drop_scd_columns(final_df)
    X = prep.generate_X(final_df)
    X = prep.encode_categories(X)
    Y = prep.generate_Y(final_df)
    # print(X.dtypes)
    path = "C:/Users/shubh/OneDrive - SRH IT/University/Semester 3/Data Management 2/Project/Output"
    model = Model(X,Y, path)
    model.DTC()
    model.SVM()
    model.RFC()
    model.ABC()


