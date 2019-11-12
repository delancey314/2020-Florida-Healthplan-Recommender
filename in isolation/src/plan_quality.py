import pandas as pd
import numpy as np

rejections = pd.read_csv('../data/quality_dbs/rejection_rates.csv')

rejections.rename(columns={'Is_Issuer_New_to_Exchange? (Yes_or_No)':'new_plan'},inplace=True )
rejections=rejections.replace('#DIV/0!','0')
rejections.Plan_denial_rate=pd.to_numeric(rejections.Plan_denial_rate)
'''
Aggregate and calculate the percent of people who no longer had coverage 
on Oct 1, 2019 who bought a policy prior to Jan 1, 2019
'''

rejections['IssuerEnroll']=rejections.groupby('Issuer_ID')['Enrollment_Data'].transform(np.sum)
rejections['I_Disenroll']=rejections.groupby('Issuer_ID')['Disenrollment_Data'].transform(np.sum)
rejections['Issuer_Disenroll']=rejections['I_Disenroll']/rejections['IssuerEnroll']*100
rejections.Issuer_Denial_rate=rejections.Issuer_Denial_rate.round(1)
rejections.Plan_denial_rate=rejections.Plan_denial_rate.round(1)
rejections.Disenrollment_Rate=rejections.Disenrollment_Rate.round(1)
rejections.Issuer_Disenroll=rejections.Issuer_Disenroll.round(1)

'''
new plans have their NaN replaced with a note stating they are new so cannot report.
Issuers who did not report the required data by the October 1 deadline have comment stating
that
'''

rejections['Plan_denial_rate'] = np.where((rejections['new_plan']=='Yes'),'New Plan',rejections['Plan_denial_rate'])
rejections['Issuer_Disenroll'] = np.where((rejections['new_plan']=='Yes'),'New Plan',rejections['Issuer_Disenroll'])
rejections['Issuer_Denial_rate'] = np.where((rejections['new_plan']=='Yes'),'New Plan',rejections['Issuer_Denial_rate'])
rejections.Disenrollment_Rate=rejections.Disenrollment_Rate.replace('nan','Disenrollment not Provided')
rejections.Issuer_Disenroll=rejections.Issuer_Disenroll.replace('nan','Disenrollment not Provided')
rejections=rejections.replace('nan','Denials not Provided')
'''
Quality and satisfaction  data is reported to a different branch of HHS so a duplicate
term has to be changed to the standard of the rest of the datasets
'''
rejections.rename(columns ={'Plan_ID':'StandardComponentId'},inplace=True)


rejections.sort_values('StandardComponentId')
rejections=rejections.drop_duplicates('Issuer_ID')
rejections=rejections.rename(columns={'Issuer_ID':'IssuerId'})
disenroll_rates=rejections[['IssuerId','Issuer_Denial_rate','Issuer_Disenroll']]
'''
satisfaction rates are imported from another HHS reporting source 
and combined with rejections
'''
quality=pd.read_csv('../data/quality_dbs/Nationwide_QRS_PUF_PY2020.csv')
quality.replace(['NG','CSR-I'],[-1,-1],inplace=True) 
quality['Global_rate']=quality['Global_rate'].astype(int)
quality_FL=quality[quality['State']== 'FL']
clean_approval=quality_FL[['IssuerID', 'Global_rate']]
clean_approval=clean_approval.rename(columns={'IssuerID':'IssuerId'})

plan_quality=disenroll_rates.merge(clean_approval, how='left', on='IssuerId')
plan_quality.at[1, 'Issuer_Disenroll'] = 'Disenrollment not Provided'
plan_quality.fillna('Not Provided')
plan_quality=plan_quality.drop_duplicates('IssuerId')

# output for model
plan_quality.to_csv('../data/clean_files/plan_quality_rates.csv')