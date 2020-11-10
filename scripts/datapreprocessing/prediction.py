
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
