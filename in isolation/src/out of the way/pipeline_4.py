import pandas as pd
import numpy as np
import sys
from sklearn.metrics.pairwise import pairwise_distances
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
    matrix=pd.read_pickle('../models/florida_jaccard.pkl')
    area_plans=area.merge(matrix, how='left', on='IssuerId')
    area_plans.drop('zipcode', axis=1, inplace=True)

    return area_plans

def make_user(short_names):
    
    customer_dict={}
    for short_name in short_names:
        customer_dict[short_name]=-1
    return customer_dict

def make_customer_row(customer_dict,customer_choices):
    for benefit in customer_choices:
        if benefit in customer_dict.keys():
             customer_dict[benefit]=1        
    return customer_dict

def jiccard_similarity(customer_record,plans):
    '''
    makes a list of all the plan names, drops the names to make a numpy matrix,
    uses the matrix to do jiccard similarity, then adds the names back on. The new
    dataframe is then clean and sorted to show the top plans.
    '''
    issuer_ids=plans['IssuerId'].to_list()
    issuer_ids=issuer_ids.append('User')
    plans.drop('IssuerId',axis=1, inplace=True)
    new_names={}
    for num, issuer in enumerate(issuer_ids):
        new_names[num]=issuer

    jaccard_matrix = plans.append(customer_record, ignore_index=True)
    jaccard_array= jaccard_matrix.to_numpy()
     jaccard_calc = 1 - pairwise_distances(jaccard_array, metric = "hamming")
    
    jaccard_similarity = pd.DataFrame(jaccard_calc)
    jaccard_similarity['Plans']=issuer_ids
    jaccard_similarity=jaccard_similarity.rename(columns=IssuerIDs)
    jaccard_similarity=jaccard_similarity.sort_values('User',ascending = False)

    jaccard_similarity=jaccard_similarity.iloc[1:]
    best_to_worst=jaccard_similarity[['Plan','User']]
    
    return best_to_worst









plans=find_plans_for_area(33101)
new_user=make_user()
customer_record = make_customer_row(new_user,customer_choices)
jaccard=jiccard_similarity(customer_record,plans)