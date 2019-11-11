import pandas as pd
import numpy as np
import sys
from sklearn.metrics.pairwise import pairwise_distances



# class Pipeline_3():

#     def __init__(self):
#         pass


def find_area(self):
    area_import = pd.read_csv('../data/clean_files/area_FL.csv')
    area=area_import.copy()
    area.drop(['Unnamed: 0','County','County_Name','ServiceAreaId'], axis=1, inplace=True)
    area.dropna(how='any', inplace=True)
    area['IssuerId']=area['IssuerId'].astype(int)
    return area

def find_benefits(self):
    benefits = pd.read_csv('../data/clean_files/benefits_covered_FL.csv')
    benefits['IssuerId']=benefits['PlanId'].str.slice(stop=5)
    benefits=benefits.drop_duplicates(['IssuerId'])
    benefits.drop(['PlanId'], axis=1, inplace=True)
    return benefits

def find_attributes(self):
    attributes = pd.read_csv('../data/clean_files/attr_FL.csv')
    attributes=attributes.drop_duplicates('IssuerId')
    #grabbing network names here instead of importing network_FL.csv
    plan_names=attributes[['IssuerId', 'Issuer_Name']]
    attributes.drop(['PlanId','Unnamed: 0','Issuer_ID','Issuer_Denial_rate','Plan_denial_rate',
    'Disenrollment_Rate','Issuer_Denial_rate','Plan_denial_rate','Disenrollment_Rate','StandardComponentId','NetworkId',
    'ServiceAreaId','PlanType','MetalLevel','PlanVariantMarketingName','Issuer_Name','No Management Program'], axis=1, inplace=True)
   
    return attributes, plan_names

def find_matrix(benefits,attributes):
    ben_attr_merged = benefits.merge(attributes, how='left', on='IssuerId')
    ben_attr_merged['Osteo']=  ben_attr_merged['Osteoporosis']+ ben_attr_merged['Osteoporosis Treatment']
    ben_attr_merged= ben_attr_merged.drop(['Osteoporosis','Osteoporosis Treatment'],axis=1)
    ben_attr_merged.rename(columns={'Osteo':'Osteoporosis'}, inplace=True)

    features= ['PlanId','Bone Marrow Transplant','Chemotherapy','Radiation',
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
    # reorganizes columns in order of flask app
    plans_matrix_df= ben_attr_merged[features]
    # pops plan labels for reference after compare and then converts it to the matrix for the compare
    indices=plans_matrix_df['IssuerId']
    plans_matrix_df.drop('PlanId',axis=1, inplace=True)
    plans_matrix_df=plans_matrix_df.astype(int)

   
    return (plans_matrix_df,indices)

    if __name__ == "__main__":
        area=find_area()
        benefits=find_benefits
        attributes, plan_names = find_attributes()
        make_matrix, plan_indexes=find_matrix(benefits, attributes)

        pass