
# Merging Tables for Flat Records
final_df = policy_fact.merge(customer_dim, on="cust_id", how="left", suffixes=["_fact","_cust"])
final_df = final_df.merge(address_dim, on="address_id", how="left", suffixes=["_fact","_add"])
final_df = final_df.merge(incidents_dim, on="incident_id", how="left", suffixes=["_fact","_inc"])
final_df = final_df.merge(policy_csl_dim, on="policy_csl_id", how="left", suffixes=["_fact","_inc"])
final_df = final_df.merge(policy_state_dim, on="policy_state_id", how="left", suffixes=["_fact","_inc"])

# Removing SCD Columns
final_df.drop(columns=['sk', 'scd_start', 'scd_end','scd_version','sk_inc', 'scd_start_inc', 'scd_end_inc',
                       'scd_version_inc','sk_fact', 'scd_start_fact', 'scd_end_fact', 'scd_version_fact',  'sk_add',
                       'scd_start_add', 'scd_end_add', 'scd_version_add',  'sk_fact', 'scd_start_fact',
                       'scd_end_fact', 'scd_version_fact','sk_cont', 'scd_start_cont', 'scd_end_cont',
       'scd_version_cont'], inplace=True)


del address_dim
del customer_dim
del incidents_dim
del policy_csl_dim
del policy_fact
del policy_state_dim


X_filter = final_df.filter(items=['number_of_vehicles_involved',
       'bodily_injuries', 'witnesses', 'total_claim_amount', 'injury_claim',
       'property_claim', 'vehicle_claim', 'months_as_customer',
         'policy_deductable', 'policy_annual_premium', 'umbrella_limit',
       'capital_gains', 'capital_loss', 'age', 'insured_zip', 'insured_sex',
       'insured_education_level', 'insured_occupation', 'insured_hobbies',
       'insured_relationship',  'city_state', 'zip',
        'incident_type', 'incident_severity',
       'authorities_contacted', 'incident_state', 'incident_city',
                           'incident_hour_of_the_day', 'property_damage',
       'police_report_available', 'collision_type',
       'policy_csl', 'policy_state'])


category = pd.DataFrame()
# Changing Type of Data Columns to Categorical and Integer in the Dataframe
X_filter['incident_hour_of_the_day'] = X_filter['incident_hour_of_the_day'].astype(int)
X = X_filter.copy()
for eachCol in ['policy_deductable','insured_sex','insured_zip','insured_education_level', 'insured_occupation', 'insured_hobbies',
'insured_relationship','city_state','zip','incident_type', 'incident_severity','authorities_contacted','incident_state',
  'incident_city', 'property_damage','police_report_available', 'collision_type','policy_csl',
      'policy_state']:
    X[eachCol] = X[eachCol].astype('category')
    cat = X[eachCol].cat.categories
    cat_len = len(X[eachCol].cat.categories)
    category = category.append(pd.DataFrame(data={'col':[eachCol],'len':[cat_len],'categories':[cat]}))

    label_encoder = LabelEncoder()
    X[eachCol] = label_encoder.fit_transform(X[eachCol])
    with open("C:/Users/shubh/OneDrive - SRH IT/University/Semester 3/Data Management 2/Project/Output/LabelEncoders/"+eachCol+'.pkl', 'wb') as output:
        pickle.dump(label_encoder, output, pickle.HIGHEST_PROTOCOL)




y = final_df.filter(items=['fraud_reported'])

# s = final_df.groupby(by=['fraud_reported'], as_index=False).count()
# s = final_df.describe()
# Split Data

# SVM
X_train, X_test, Y_train, Y_test = train_test_split(X, y, test_size=0.3, random_state=42)
# clf = SGDClassifier(loss="hinge", penalty="l2")
clf = SGDClassifier(n_jobs=6, loss='hinge', penalty='l2', alpha=1000, random_state=69, max_iter=1000, tol=None)
clf.fit(X_train, Y_train.values.ravel())
score = clf.score(X_test, Y_test)
print("SVM" ,score)

# DecisionTreeClassifier
clf_dt = DecisionTreeClassifier(max_depth=5)
clf_dt.fit(X_train, Y_train.values.ravel())
score_dt = clf_dt.score(X_test, Y_test)
print("Decison Tree" ,score_dt)

clf_rf = RandomForestClassifier(max_depth=5, n_estimators=100)
clf_rf.fit(X_train, Y_train.values.ravel())
score_rf = clf_rf.score(X_test, Y_test)
print("Random Forest" ,score_rf)

clf_ab = AdaBoostClassifier(n_estimators=100)
clf_ab.fit(X_train, Y_train.values.ravel())
score_ab = clf_ab.score(X_test, Y_test)
print("Ada Boost" ,score_ab)


# Prediction Code
predict_value = X_filter[:1:]


predict_value = predict_value.filter(items=['number_of_vehicles_involved',
       'bodily_injuries', 'witnesses', 'total_claim_amount', 'injury_claim',
       'property_claim', 'vehicle_claim', 'months_as_customer',
         'policy_deductable', 'policy_annual_premium', 'umbrella_limit',
       'capital_gains', 'capital_loss', 'age', 'insured_zip', 'insured_sex',
       'insured_education_level', 'insured_occupation', 'insured_hobbies',
       'insured_relationship',  'city_state', 'zip',
        'incident_type', 'incident_severity',
       'authorities_contacted', 'incident_state', 'incident_city',
                           'incident_hour_of_the_day', 'property_damage',
       'police_report_available', 'collision_type',
       'policy_csl', 'policy_state'])

predict_value['incident_hour_of_the_day'] = predict_value['incident_hour_of_the_day'].astype(int)

for eachCol in ['policy_deductable','insured_sex','insured_zip','insured_education_level', 'insured_occupation', 'insured_hobbies',
'insured_relationship','city_state','zip','incident_type', 'incident_severity','authorities_contacted','incident_state',
  'incident_city', 'property_damage','police_report_available', 'collision_type','policy_csl',
      'policy_state']:
    #print(eachCol)
    predict_value[eachCol] = predict_value[eachCol].astype('category')
    with open("C:/Users/shubh/OneDrive - SRH IT/University/Semester 3/Data Management 2/Project/Output/LabelEncoders/"+eachCol+'.pkl', "rb") as input_file:
        e = pickle.load(input_file)
        predict_value[eachCol] = e.transform(predict_value[eachCol])


# print(predict_value)

print(clf.predict(predict_value))
print(clf_dt.predict(predict_value))
print(clf_rf.predict(predict_value))
print(clf_ab.predict(predict_value))

