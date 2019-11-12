import pandas as pd
import numpy as np
import sys
from sklearn.metrics.pairwise import pairwise_distances

class FloridaPipeline():

    def __init__(self):
        
        self.short_names=['bone','chemo','rad','cpr','hrtman','bpcho','diab_care',
                  'diab_edu','dialysis','infus','gene','pict','lab','xray','glass_a','glass_c',
                  'eye_a','eye_c','dme','g_tube','hab','home','hospice','pain','osteo',
                  'prosth','skill_rn','er','ambu','hosp','inpt','all_inj','all_test','breath',
                  'prevent','transplant','l_d','nutr','preg_m','prenat','well_b','depr','ment_off',
                  'ment_in','ment_out','not_doc','out_surg','out_ambu','pcp','foot','spec','tele','urgent',
                  'back','observ','rehab_out','reconst','pt_rehab','speech','drug_in','drug_out',
                  'drug_off','chiro','fit','gym','o2','nutr_counc','tmj','kg_m']
        self.features = ['Bone Marrow Transplant','Chemotherapy','Radiation',
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
        self.rename_dict=zip(self.features,self.short_names)

    def make_plan_matrix(self):
        '''
        imports needed the plan detail files, merges them, reorders to match 
        the flask app, then outputs two versions. 

            self.plans_matrix_df is kept with the index and column names so it 
            can be referred back to provide the plan names.

            self.plans_matrix is transformed to a matrix for actual calculations.

        '''

        self.attr = pd.read_csv('../data/clean_files/attr_FL.csv')
        self.ben = pd.read_csv('../data/clean_files/benefits_covered_FL.csv')

        #self.attr.drop(['Unnamed: 0','Issuer_ID'], axis=1, inplace=True)
        self.attr.drop(['Issuer_Denial_rate','Plan_denial_rate','Disenrollment_Rate'], axis=1, inplace=True)


        self.attr=self.attr.sort_values(by=['IssuerId', 'ServiceAreaId'])
        merged_ben_attr = self.ben.merge(self.attr, how='left', on='PlanId')
        merge_clean= merged_ben_attr.dropna()
        merge_clean['Osteo']= merge_clean['Osteoporosis']+merge_clean['Osteoporosis Treatment']

        self.merge_clean3=merge_clean.drop(['Osteoporosis','Osteoporosis Treatment','StandardComponentId','IssuerId','NetworkId',
                                    'ServiceAreaId','PlanType','MetalLevel','PlanVariantMarketingName','Issuer_Name',
                                    'No Management Program'],axis=1)

        self.merge_clean3.rename(columns={'Osteo':'Osteoporosis'}, inplace=True)
        '''
            this will replace features - exclusions are misbehaving so they have
            been commented out of flask

            new_cols_all = ['PlanId','Bone Marrow Transplant','Chemotherapy','Radiation',
            'Cardiac and Pulmonary Rehabilitation','Heart Disease Management Program',
            'High Blood Pressure & High Cholesterol Management Program','Diabetes Care Management',
            'Diabetes Education','Dialysis','Infusion Therapy','Genetic Testing Lab Services',
            'Imaging (CT/PET Scans, MRIs)','Laboratory Outpatient and Professional Services',
            'X-rays and Diagnostic Imaging','Eye Glasses for Adults','Eye Glasses for Children',
            'Routine Eye Exam (Adult)','Routine Eye Exam for Children',
            'Durable Medical Equipment','Enteral/Parenteral and Oral Nutrition Therapy',
            'Habilitation Services','THIS IS WHERE HEARING AIDS GOES',
            'Home Health Care Services','Hospice Services',
            'THIS IS WHERE LONG TERM NURSING GOES','Pain Management Program','THIS IS WHERE PRIVATE NURSE GOES',
            'Osteoporosis','Prosthetic Devices','Skilled Nursing Facility',
            'Emergency Room Services','Emergency Transportation/Ambulance',
            'Inpatient Hospital Services (e.g., Hospital Stay)','Inpatient Physician and Surgical Services',
            'Allergy Injections','Allergy Testing','Asthma Management Program',
            'Preventive Care/Screening/Immunization','Transplant','ABORTION GOES HERE',
            'Delivery and All Inpatient Services for Maternity Care','INFERTILITY GOES HERE',
            'NEWBORN HEARING GOES HERE', 'NEWBORN OTHER GOES HERE','Nutrition/Formulas',
            'Pregnancy Management Program','Prenatal and Postnatal Care',
            'Well Baby Visits and Care','Depression Management Program',
            'Mental Health Office Visit','Mental/Behavioral Health Inpatient Services',
            'Mental/Behavioral Health Outpatient Services', 'MENTAL HEALTH OTHER GOES HERE',
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
            'ACUPUNCTURE GOES HERE','BARIATRIC GOES HERE','Chiropractic Care','COSMETIC GOES HERE',
            'Fitness Center Membership','Gym Access','Hyperbaric Oxygen Therapy','Nutritional Counseling',
            'Treatment for Temporomandibular Joint Disorders','Weight Loss Management Program']
        '''
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
        self.plans_matrix_df= merge_clean3[features]

        #plans_matrix_df.set_index('PlanId')
        self.plans_matrix_df.drop('PlanId',axis=1, inplace=True)
        self.plans_matrix_df=self.plans_matrix_df.astype(int)
        
    def new_customer(self):
        self.customer_dict={}
        for short_name in self.short_names:
            self.customer_dict[short_name]=-1
       

    def make_customer_row(self):
        for benefit in self.customer_choices:
            if benefit in self.customer_dict.keys:
                self.customer_dict[benefit]=1
        

    def benefit_dict(self):
        self.benefit_dict= dict(zip(self.short_names, self.features))

    def jaccard(self):
        self.jaccard_matrix = self.plans_matrix.append(self.customer_dict)
        jaccard_calc = 1 - pairwise_distances(self.jaccard_matrix, metric = "hamming")
        self.jaccard_similarity = pd.DataFrame(jaccard_calc, index=self.plans_matrix.index, columns=plans_matrix.index)

        self.match = self.jaccard_similarity.iloc[[-1]]


    def narrow_plans(self,zip):
        self.zip=zip

        pass


    def analysis(self,listx):
        self.customer_choices=listx
        make_plan_matrix()
        new_customer()
        make_customer_row()
        self.benefit_dict()
        jaccard()
        return self.match






if __name__ == "__main__":
    example=[1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1,-1,1]
    xz=FloridaPipeline()
    frank=xz.analysis(example)
    #list_of_plans=make_plan_matrix()
    #plans_matrix = list_of_plans.drop('PlanId', axis=1)
    #benefits=list_of_plans.columns()
    #customer_dict=new_customer()
    pass