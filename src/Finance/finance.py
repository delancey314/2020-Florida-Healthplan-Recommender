'''
This script is not in use for the baseline FL project because the rates cannot be 
accurately mapped back to a zipcode.

For the other files the plans are mapped to given area, an issuer can have up t
o 37 plans for an area, 1 per county. They are givena unique planIds for each. 

For the finance data, they use a different column 'RateArea' which does not match to a 
geographic area or a plan.  Since the rates are different, an accurate estimate
cannot be mapped.

A request has been made of a cross table from CMS.
'''



def tax_credit(income=52594, fam_size=3, lowest_2 = 7954):
    '''
    Calculates the tax credit for the enrollee for their income status
    and location.  The threshold and cutoffs for each level are set 
    annually by congress as part of the HHS budget bill so this will need to be
    updated in 2020 after the next version is approved.
     '''
     
    poverty_threshold = 12490 + 4420*(fam_size-1)
    percent_poverty = income/poverty_threshold
    if  percent_poverty <= 1.33:
        rate = .0206
    elif percent_poverty <= 1.5:
        rate = .0309+(((percent_poverty-1.33)/(1.5-1.33))*(.0412-.0309))
    elif percent_poverty <= 2:
        rate = .0412+(((percent_poverty-1.5)/(2-1.5))*(.0649-.0412))
    elif percent_poverty <= 2.5:
        rate = .0649+(((percent_poverty-2)/(2.5-2))*(.0829-.0649))
    elif percent_poverty <= 3:
        rate = .0829+(((percent_poverty-2.5)/(3-2.5))*(.0978-.0829))
    elif percent_poverty <= 4:
        rate = .0978
    else:
        return 0
        
    return lowest_2*(1-rate)

rate=pd.read_csv('../data/Rate_PUF.csv',low_memory=False, encoding ='latin1')
'''
no value - 'BusinessYear','SourceName', 'ImportDate'
found elsewhere - FederalTIN

'''
rate=rate.drop(['BusinessYear','SourceName', 'ImportDate','FederalTIN'], axis=1)

'''
There are rates dependent on which quarter coverage will begin
['2020-01-01', '2020-07-01', '2020-04-01', '2020-10-01']
For the initial project, will assume 1st quarter only.
Rate offerings can be good for only one quarter, 6 months, or the year so they expire
on certain dates so new rate plan ca replace. 
For the initial project, will assume 1st quarter only for all signups
'''

rate=rate[rate.RateEffectiveDate == '2020-01-01']
rate=rate.drop('RateExpirationDate', axis=1)

rate_FL=rate[rate.StateCode == 'FL'].copy()
rate_FL.info()



   
