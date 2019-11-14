# move this to '../app/app' before running - templates, etc  are there

from flask import Flask, request, render_template, session, redirect
import pandas as pd
import numpy as np
import sys
from sklearn.metrics.pairwise import pairwise_distances

app = Flask(__name__)



'''
This uses the cleaned data created by other scripts to do the work needed while the flask app
is running. Please see the readme.md @git/delancey314 for details
'''

#names that match flask
short_names=['bone','chemo','rad','cpr','hrtman','bpcho','diab_care',
                  'diab_edu','dialysis','infus','gene','pict','lab','xray','glass_a','glass_c',
                  'eye_a','eye_c','dme','g_tube','hab','home','hospice','pain','osteo',
                  'prosth','skill_rn','er','ambu','hosp','inpt','all_inj','all_test','breath',
                  'prevent','transplant','l_d','nutr','preg_m','prenat','well_b','depr','ment_off',
                  'ment_in','ment_out','not_doc','out_surg','out_ambu','pcp','foot','spec','tele','urgent',
                  'back','observ','rehab_out','reconst','pt_rehab','speech','drug_in','drug_out',
                  'drug_off','chiro','fit','gym','o2','nutr_counc','tmj','kg_m']
#names that match everything else
features = ['Bone Marrow Transplant','Chemotherapy','Radiation',
                 'Cardiac and Pulmonary Rehabilitation','Heart Disease Management Program',
                 'High Blood Pressure & High Cholesterol Management Program','Diabetes Care Management',
                 'Diabetes Education','Dialysis','Infusion Therapy','Genetic Testing Lab Services',
                 'Imaging (CT/PET Scans, MRIs)','Laboratory Outpatient and Professional Services',
                 'X-rays and Diagnostic Imaging','Eye Glasses for Adults','Eye Glasses for Children',
                 'Routine Eye Exam (Adult)','Routine Eye Exam for Children',
                 'Durable Medical Equipment','Enteral/Parenteral and Oral Nutrition Therapy',
                 'Habilitation Services','Home Health Care Services','Hospice Services',
                 'Pain Management Program','Osteoporosis','Prosthetic Devices',
                 'Skilled Nursing Facility','Emergency Room Services',
                 'Emergency Transportation/Ambulance',
                 'Inpatient Hospital Services (e.g., Hospital Stay)',
                 'Inpatient Physician and Surgical Services','Allergy Injections',
                 'Allergy Testing','Asthma Management Program',
                 'Preventive Care/Screening/Immunization','Transplant',
                 'Delivery and All Inpatient Services for Maternity Care','Nutrition/Formulas',
                 'Pregnancy Management Program','Prenatal and Postnatal Care',
                 'Well Baby Visits and Care','Depression Management Program',
                 'Mental Health Office Visit','Mental/Behavioral Health Inpatient Services',
                 'Mental/Behavioral Health Outpatient Services',
                 'Other Practitioner Office Visit (Nurse, Physician Assistant)',
                 'Outpatient Facility Fee (e.g., Ambulatory Surgery Center)',
                 'Outpatient Surgery Physician/Surgical Services',
                 'Primary Care Visit to Treat an Injury or Illness','Routine Foot Care',
                 'Specialist Visit','Telehealth','Urgent Care Centers or Facilities',
                 'Low Back Pain Management Program','Outpatient Observation',
                 'Outpatient Rehabilitation Services','Reconstructive Surgery',
                 'Rehabilitative Occupational and Rehabilitative Physical Therapy',
                 'Rehabilitative Speech Therapy','Substance Abuse Disorder Inpatient Services',
                 'Substance Abuse Disorder Outpatient Services','Substance Abuse Office Visit',
                 'Chiropractic Care','Fitness Center Membership','Gym Access',
                 'Hyperbaric Oxygen Therapy','Nutritional Counseling',
                 'Treatment for Temporomandibular Joint Disorders',
                 'Weight Loss Management Program']

rename_dictionary = dict(zip(short_names,features))


def find_plans_for_area(zip):
    area_import = pd.read_csv('../data/clean_files/area_FL.csv')
    area=area_import.copy()
    area.drop(['Unnamed: 0','County','County_Name','ServiceAreaId'], axis=1, inplace=True)
    area.dropna(how='any', inplace=True)
    area['IssuerId']=area['IssuerId'].astype(int)
    area=area[area['zipcode'] ==zip]
    area=area.drop_duplicates(['IssuerId'])
    #import model and use a left merge to select the correct plans for the area
    matrix=pd.read_pickle('../models/florida_jaccard.pkl')
    area_plans=area.merge(matrix, how='left', on='IssuerId')
    area_plans.drop('zipcode', axis=1, inplace=True)

    return area_plans

def make_user(short_names):
    '''
    initialize user on first log-in with -1 for all values. This is to avoid
    mapping plans on the zeros. In Jaccard similarity matching zeros counts the
    same as matching 1. The sparcity of the matrix gaurantees matching on the zeros
    left as default.
    '''
    
    customer_dict={}
    for short_name in short_names:
        customer_dict[short_name]=-1
    return customer_dict

def make_customer_row(customer_dict,customer_choices):
    #convert the customer choices to 1 so they can match the plan benefits
    for benefit in customer_choices:
        if benefit in customer_dict.keys():
             customer_dict[benefit]=1        
    return customer_dict

def jiccard_similarity(customer_record,plans):
    '''
    makes a list of all the plan names, drops the names to make a numpy matrix,
    uses the matrix to do jiccard similarity, then adds the names back on. The new
    dataframe is then cleaned and sorted to show the top plans.
    '''
    issuer_ids=plans['IssuerId'].to_list()
    
    issuer_ids.append('jaccard_score')
   
    plans.drop('IssuerId',axis=1, inplace=True)
    new_names={}
    for num, issuer in enumerate(issuer_ids):
        new_names[num]=issuer
  
    jaccard_matrix = plans.append(customer_record, ignore_index=True)

    jaccard_array= jaccard_matrix.to_numpy()
    jaccard_calc = 1 - pairwise_distances(jaccard_array, metric = "hamming")
    
    jaccard_similarity = pd.DataFrame(jaccard_calc)
    jaccard_similarity['IssuerId']=issuer_ids
    jaccard_similarity=jaccard_similarity.rename(columns=new_names)
    jaccard_similarity=jaccard_similarity.sort_values('jaccard_score',ascending = False)
    jaccard_similarity=jaccard_similarity.iloc[1:]
    best_to_worst=jaccard_similarity[['IssuerId','jaccard_score']]
   
    
    
    return best_to_worst

def add_quality(plan_list):
    '''
    Imports and attaches claim denials, customer rankings, and disenrollmment
    to the top plans before returning to the customer.
    '''
    
    quality=pd.read_csv('../data/clean_files/plan_quality_rates.csv')
    final_merge = plan_list.merge(quality,how='left',on='IssuerId')
    final_merge.drop(['Unnamed: 0','IssuerId'], axis=1,inplace=True)
    return final_merge

def new_pipeline(listx,zip=33101):
    '''
    all actions to take customer input in and return the best plans.
    '''
    # test data
    #zip =33101
    # example=['bone','chemo','rad','cpr','hrtman','bpcho','diab_care',
    #               'diab_edu','dialysis','infus','gene','pict','lab','xray','glass_a','glass_c',
    #               'eye_a','eye_c','dme','g_tube','hab','home','hospice','pain','osteo',
    #               'prosth','skill_rn','er','ambu','hosp','inpt','all_inj','all_test','breath',
    #               'prevent','transplant','l_d','nutr','preg_m','prenat','well_b','depr','ment_off',
    #               'ment_in','ment_out','not_doc','out_surg','out_ambu','pcp','foot','spec','tele','urgent',
    #               'back','observ','rehab_out','reconst','pt_rehab','speech','drug_in','drug_out',
    #               'drug_off','chiro','fit','gym','o2','nutr_counc','tmj','kg_m']

    plans=find_plans_for_area(zip)
    new_user=make_user(short_names)
    customer_record = make_customer_row(new_user,listx)
    top_plans=jiccard_similarity(customer_record,plans)
    plans_rated=add_quality(top_plans)
    return plans_rated

# first landing page
@app.route('/', methods=['GET', 'POST'])
def index():
    return render_template('landing.html')

# Get's the customer's desired benefits.
@app.route('/conditions', methods=['GET', 'POST'])
def conditions():
    return render_template('conditions_experiment.html')

# takes the customer's input, does the jaccard, then returns the results.
@app.route('/analysis', methods=['GET', 'POST'])
def analysis():
    if request.method == 'POST':
        zip= request.form['zip']
        zip = int(zip)
        listx = request.form.getlist('mycheckbox')
        results=new_pipeline(listx,zip)
        print(results)

    #return 'Done'
    return render_template('results.html',tables=[results.to_html(classes='data')], titles=results.columns.values)

'''
This page is used for determining rates and costs. Since the rate cannot be
mapped back to the benefits plans at this time, it is commented out

@app.route('/demography',methods=['GET', 'POST'])
def demography():
    return render_template('landing.html')
'''

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=8105, threaded=True, debug=True)

  