import pandas as pd 
import numpy as np 


#this gets rid of the Dental Only Plans leaving medical insurance plans then drops the column as superfilous
area=area[area.DentalOnlyPlan != 'Yes']
area.drop('DentalOnlyPlan', axis=1, inplace=True)

area=area.drop(['BusinessYear','SourceName', 'ImportDate'], axis = 1)
area.info()

class Pipeline(self):
    

    def __init__(self):
        self.fl= True
        self.load_file=None
        self.workfile=None
        self.file_list = None # make_files() will be updated with a list

    




    def clean_all(self):
        #sets all files to Florida only
        if  self.fl:
            self.workfile=self.workfile[self.workfile.StateCode == 'FL'].copy()

        #this gets rid of the Dental Only Plans leaving medical insurance plans then drops the column as superfilous
        self.workfile= self.workfile[ self.workfile.DentalOnlyPlan != 'Yes']
        self.workfile.drop('DentalOnlyPlan', axis=1, inplace=True)
        #extra columns in all files
        self/workfile=self.workfile.drop(['BusinessYear','SourceName', 'ImportDate'], axis = 1)
        self.workfile['IssuerId']=self.workfile['IssuerId'].astype(int)
    
    def  make_dummies(self):
        # I have lost my script for this. Need to replace.
        pass

    def clean_area(self):
        '''
        Imports the national plan DB of what areas plans belong to, cleans it, and then combines it with the FIPS
        DB to provide a 4 column reference dataframe for determining which plans to limit by geography
        '''
        self.workfile=pd.read_csv('../data/raw_files/Service_Area_PUF.csv',low_memory=False, encoding ='latin1')
        clean_all()
        
        if self.fl:
            #Florida does not have plans at the subcounty level
            self.workfile=self.workfile.drop(['PartialCounty', 'ZipCodes', 'PartialCountyJustification'], axis = 1)
            #FLorida only has individual plans so column is not needed
            self.workfile=self.workfile.drop('MarketCoverage', axis = 1)
            in_state= self.workfile[self.workfile.CoverEntireState== 'Yes']

        level_plans()

        #dropna has to be done after levelling or plans are lost
        self.workfile=self.workfile.dropna()
        #Some states reset 'IssuerId' after the droppna. Code below is work around
        #self.workfile['IssuerId']=self.workfile['IssuerId'].astype(int)
        self.workfile['County']=self.workfile['County'].astype(int)
        self.workfile.drop(['StateCode','ServiceAreaName','CoverEntireState'],axis=1,inplace=True)

        '''
        imports Federal DB of geographic codes and cross references them to the correct level and outputs 
        appropriate tables for lookup
        '''

        fips= pd.read_csv('../data/zipcodes-county-fips-crosswalk/ZIP-COUNTY-FIPS_2017-06.csv',  dtype='str')
        fips['STCOUNTYFP']=fips['STCOUNTYFP'].astype(int)
        fips['COUNTYNAME'] = fips['COUNTYNAME'].str.replace(r' County', '')
        fips = fips[fips.STATE== 'FL']
        zips =pd.DataFrame()
        zips[['zipcode','County','County_Name']] = fips[['ZIP','STCOUNTYFP','COUNTYNAME']]
        zips.to_csv('../data/clean_files/zipcodes.csv')
        FL_county =pd.DataFrame()
        FL_county[['County','County_Name']] = fips[['STCOUNTYFP','COUNTYNAME']]
        FL_county.sort_values(by=['County'])
        FL_county=FL_county.drop_duplicates(keep='first')
        result = pd.merge(self.self.workfile,FL_county, how='left', on=['County', 'County'])
        result.to_csv('../data/clean_files/area_FL.csv')

    def level_plans(self):
        '''
        converts plans to their lowest level needed (State->multiple county->county->
        -> partial county->zipcode
        '''
        state_plans = ['FL','16842','FLS001','BlueOptions','Yes']
        uniques={}
        for col in self.workfile.columns: 
            uniques[col]=self.workfile[col].unique()
            county_list=uniques['County']

        for county in county_list:
            state_plans.append(county)
            self.workfile = self.workfile.append(pd.Series(state_plans, index=self.workfile.columns ), ignore_index=True)
    
    def attribute_exclusions():
        # placeholder for now
        pass

    def clean_attributes(self):
        self.workfile=pd.read_csv('../data/raw_files/Plan_Attributes_PUF.csv',low_memory=False, encoding ='latin1')
        clean_all()
        
        if self.fl != True:
            #Florida does not have any attribute exclusions. Most states do.
            attribute_exclusions()

        # no value for recommender at this time
        self.workfile=self.workfile.drop(['BusinessYear','SourceName', 'ImportDate',
                'PlanExpirationDate','PlanEffectiveDate',
                'URLForEnrollmentPayment', 'FormularyURL','IsGuaranteedRate'], axis=1)
        #These are either 0, N/A, or No for all values in FL
        self.workfile=self.workfile.drop(['IndianPlanVariationEstimatedAdvancedPaymentAmountPerEnrollee',
                'ChildOnlyPlanId','HSAOrHRAEmployerContribution', 
                'HSAOrHRAEmployerContributionAmount','PlanLevelExclusions'], axis = 1)
        #These are alterations to default rates.  rate file cannot be used at this time.

        self.workfile=self.workfile.drop(['MEHBInnTier1IndividualMOOP','MEHBInnTier1FamilyPerPersonMOOP',
                'MEHBInnTier1FamilyPerGroupMOOP','MEHBInnTier2IndividualMOOP',
                'MEHBInnTier2FamilyPerPersonMOOP','MEHBInnTier2FamilyPerGroupMOOP',
                'MEHBOutOfNetIndividualMOOP','MEHBOutOfNetFamilyPerPersonMOOP',
                'MEHBOutOfNetFamilyPerGroupMOOP','MEHBCombInnOonIndividualMOOP',
                'MEHBCombInnOonFamilyPerPersonMOOP','MEHBCombInnOonFamilyPerGroupMOOP',
                'DEHBInnTier1IndividualMOOP','DEHBInnTier1FamilyPerPersonMOOP',
                'DEHBInnTier1FamilyPerGroupMOOP','DEHBInnTier2IndividualMOOP',
                'DEHBInnTier2FamilyPerPersonMOOP','DEHBInnTier2FamilyPerGroupMOOP',
                'DEHBOutOfNetIndividualMOOP','DEHBOutOfNetFamilyPerPersonMOOP',
                'DEHBOutOfNetFamilyPerGroupMOOP','DEHBCombInnOonIndividualMOOP',
                'DEHBCombInnOonFamilyPerPersonMOOP','DEHBCombInnOonFamilyPerGroupMOOP',
                'TEHBInnTier1IndividualMOOP','TEHBInnTier1FamilyPerPersonMOOP',
                'TEHBInnTier1FamilyPerGroupMOOP','TEHBInnTier2IndividualMOOP',
                'TEHBInnTier2FamilyPerPersonMOOP','TEHBInnTier2FamilyPerGroupMOOP',
                'TEHBOutOfNetIndividualMOOP','TEHBOutOfNetFamilyPerPersonMOOP',
                'TEHBOutOfNetFamilyPerGroupMOOP','TEHBCombInnOonIndividualMOOP',
                'TEHBCombInnOonFamilyPerPersonMOOP','TEHBCombInnOonFamilyPerGroupMOOP',
                'MEHBDedInnTier1Individual','MEHBDedInnTier1FamilyPerPerson',
                'MEHBDedInnTier1FamilyPerGroup','MEHBDedInnTier1Coinsurance',
                'MEHBDedInnTier2Individual','MEHBDedInnTier2FamilyPerPerson',
                'MEHBDedInnTier2FamilyPerGroup','MEHBDedInnTier2Coinsurance',
                'MEHBDedOutOfNetIndividual','MEHBDedOutOfNetFamilyPerPerson',
                'MEHBDedOutOfNetFamilyPerGroup','MEHBDedCombInnOonIndividual',
                'MEHBDedCombInnOonFamilyPerPerson','MEHBDedCombInnOonFamilyPerGroup',
                'DEHBDedInnTier1Individual','DEHBDedInnTier1FamilyPerPerson',
                'DEHBDedInnTier1FamilyPerGroup','DEHBDedInnTier1Coinsurance',
                'DEHBDedInnTier2Individual','DEHBDedInnTier2FamilyPerPerson',
                'DEHBDedInnTier2FamilyPerGroup','DEHBDedInnTier2Coinsurance',
                'DEHBDedOutOfNetIndividual','DEHBDedOutOfNetFamilyPerPerson',
                'DEHBDedOutOfNetFamilyPerGroup','DEHBDedCombInnOonIndividual',
                'DEHBDedCombInnOonFamilyPerPerson','DEHBDedCombInnOonFamilyPerGroup',
                'TEHBDedInnTier1Individual','TEHBDedInnTier1FamilyPerPerson',
                'TEHBDedInnTier1FamilyPerGroup','TEHBDedInnTier1Coinsurance',
                'TEHBDedInnTier2Individual','TEHBDedInnTier2FamilyPerPerson',
                'TEHBDedInnTier2FamilyPerGroup','TEHBDedInnTier2Coinsurance',
                'TEHBDedOutOfNetIndividual','TEHBDedOutOfNetFamilyPerPerson',
                'TEHBDedOutOfNetFamilyPerGroup','TEHBDedCombInnOonIndividual',
                'TEHBDedCombInnOonFamilyPerPerson','TEHBDedCombInnOonFamilyPerGroup'], axis =1)

        #Per data dictionary, these are actuarial columns.
        self.workfile.drop(['MarketCoverage', 'TIN','HIOSProductId', 'HPID', 'FormularyId', 'IsNewPlan',
            'DesignType', 'UniquePlanDesign', 'QHPNonQHPTypeId',
            'CompositeRatingOffered', 'EHBPercentTotalPremium',
            'EHBPediatricDentalApportionmentQuantity', 'OutOfCountryCoverage',
            'OutOfCountryCoverageDescription', 'OutOfServiceAreaCoverage',
            'OutOfServiceAreaCoverageDescription',
            'CSRVariationType', 'IssuerActuarialValue',
            'AVCalculatorOutputNumber', 'MedicalDrugDeductiblesIntegrated',
            'MedicalDrugMaximumOutofPocketIntegrated', 'MultipleInNetworkTiers',
            'FirstTierUtilization', 'SecondTierUtilization',
            'SpecialtyDrugMaximumCoinsurance', 'InpatientCopaymentMaximumDays',
            'BeginPrimaryCareCostSharingAfterNumberOfVisits',
            'BeginPrimaryCareDeductibleCoinsuranceAfterNumberOfCopays',
            'IsHSAEligible', ],axis=1,inplace=True)

        '''
        The next block is for pulling information to be used elsewhere.
        self.fronters can be combined with the other lists and then merged into
        another df.
        self.special is specialist prior approval.
        self.ld_diab_fx_mon is financial costs of of giving birth
        self.plan_docs is web links
        '''
    
        self.fronters=self.workfile[['IssuerId', 'StandardComponentId', 'PlanMarketingName', 'NetworkId',
    'ServiceAreaId','PlanId']]

        self.special=self.workfile[['IsNoticeRequiredForPregnancy','IsReferralRequiredForSpecialist','SpecialistRequiringReferral']]
        self.workfile.drop(['IsNoticeRequiredForPregnancy','IsReferralRequiredForSpecialist','SpecialistRequiringReferral'], axis=1, inplace=True)

        self.ld_diab_fx_mon=self.workfile[['SBCHavingaBabyDeductible',
            'SBCHavingaBabyCopayment', 'SBCHavingaBabyCoinsurance',
            'SBCHavingaBabyLimit', 'SBCHavingDiabetesDeductible',
            'SBCHavingDiabetesCopayment', 'SBCHavingDiabetesCoinsurance',
            'SBCHavingDiabetesLimit', 'SBCHavingSimplefractureDeductible',
            'SBCHavingSimplefractureCopayment',
            'SBCHavingSimplefractureCoinsurance', 'SBCHavingSimplefractureLimit']]
        self.workfile.drop(['SBCHavingaBabyDeductible',
            'SBCHavingaBabyCopayment', 'SBCHavingaBabyCoinsurance',
            'SBCHavingaBabyLimit', 'SBCHavingDiabetesDeductible',
            'SBCHavingDiabetesCopayment', 'SBCHavingDiabetesCoinsurance',
            'SBCHavingDiabetesLimit', 'SBCHavingSimplefractureDeductible',
            'SBCHavingSimplefractureCopayment',
            'SBCHavingSimplefractureCoinsurance', 'SBCHavingSimplefractureLimit'],axis=1,inplace=True)

        self.plan_docs=[['URLForSummaryofBenefitsCoverage','PlanBrochure']]
        self.workfile.drop(['URLForSummaryofBenefitsCoverage','PlanBrochure'],axis=1,inplace=True)
        '''
        Issuers only have to report if they have a disease management support progam 
        and then in another column list what they are. The blanks are given a 'No' which is 
        later used for 'No management program in hotcoding
        '''
        self.workfile.fillna('No', inplace=True)
        # duplicate name issue fix
        self.workfile.replace(['Weight Loss Management Program','High Blood Pressure & High Cholesterol Management Program',],
        [' Weight Loss Programs',' Blood Pressure-Cholesterol Management Program'],inplace=True)
        
        '''
        A plan can have multiple disase management plans listed in the same column.
        This code block separates them out,then adds back to the original as 
        new columns
        '''
        self.workfile= = pd.concat([self.workfile['KEYS'], self.workfile['DiseaseManagementProgramsOffered'].str.split(', ', expand=True)], axis=1)
        self.workfile.drop(['WellnessProgramOffered','DiseaseManagementProgramsOffered'], axis=1, inplace=True)
        self.workfile.to_csv('../data/clean_files/attr_FL.csv')


    def clean_benefits(self):
        self.workfile=pd.read_csv('../data/raw_files/Benefits_Cost_Sharing_PUF.csv',low_memory=False, encoding ='latin1')

        self.workfile=self.workfile.drop(['BusinessYear','SourceName', 'ImportDate'], axis = 1)
        self.workfile=self.workfile[self.workfile.StateCode == 'FL'].copy()

        
        self.workfile.drop(columns=['CopayInnTier1', 'CopayInnTier2', 'CopayOutofNet', 'CoinsInnTier1','CoinsInnTier2', 'CoinsOutofNet', 'IsEHB',
                        'QuantLimitOnSvc', 'LimitQty', 'LimitUnit', 'Explanation','EHBVarReason', 'IsExclFromInnMOOP', 'IsExclFromOonMOOP'], axis=1, inplace=True)

        dental = ['Basic Dental Care - Child', 'Orthodontia - Child', 'Major Dental Care - Child',
                'Basic Dental Care - Adult', 'Orthodontia - Adult', 'Major Dental Care - Adult',
                'Dental Check-Up for Children', 'Routine Dental Services (Adult)', 'Accidental Dental',
                'Dental Anesthesia','Congenital Anomaly, including Cleft Lip/Palate', 'Dental X-rays',
                'Topical Flouride', 'Sealants', 'Fillings', 'Recementation of Space Maintainers', 
                'Removal of Fixed Space Maintainers', 'Restorative Services', 
                'Periodontal Root Scaling and Planing', 'Periodontal Maintenance', 'Periodontal and Osseous Surgery',
                'Occlusal Adjustments', 'Root Canal Therapy and Retreatment', 'Periradicular Surgical Procedures',
                'Partial Pulpotomy', 'Vital Pulpotomy', 'Denture Adjustments',
                'Initial Placement of Bridges and Dentures', 'Tissue Conditioning', 'Reline and Rebase', 
                'Post and Core Build-up', 'Extractions', 'Complex Oral Surgery', 'Implants',
                'Immediate Dentures', 'Anesthesia Services for Dental Care', 'Accidental Dental Adult','Denture Reline and Rebase']

        drugs = ['Off Label Prescription Drugs', 'Generic Drugs', 'Preferred Brand Drugs','Non-Preferred Brand Drugs','Specialty Drugs',
                'Tier 2 Generic Drugs', 'Preferred Generic Drugs']
        self.workfile=self.workfile[self.workfile['BenefitName'].isin(dental) == False]
        make_dummies(self)
        self.workfile.to_csv('../data/clean_files/ben_FL.csv')

    def clean_network(self):
        self.workfile=pd.read_csv('../data/raw_files/Network_PUF.csv',low_memory=False, encoding ='latin1')
        clean_all()
        self.workfile=self.workfile.drop(['BusinessYear','SourceName', 'ImportDate','NetworkURL'], axis = 1)
        self.workfile.drop(['StateCode','MarketCoverage'],axis=1, inplace=True)
        self.workfile=self.workfile[['IssuerId','NetworkId','NetworkName']]
        temp_area=pd.read_csv('../data/clean_files/area_FL.csv')
        self.workfile=temp_area.merge(self.workfile, how='left', on='IssuerId')
        self.workfile.to_csv('../data/clean_files/network_FL.csv')

    def clean_rules():
        '''
        Financial  data cannot be linked to benefit plans at this time so 
        this method is not used
        '''
        self.workfile=pd.read_csv('../data/raw_files/Business_Rules_PUF.csv',low_memory=False, encoding ='latin1')
        clean_all()
        pass
    
    def clean_rates():
        '''
        Financial  data cannot be linked to benefit plans at this time so 
        this method is not used
        '''
        self.workfile=pd.read_csv('../data/raw_files/Rate_PUF.csv',low_memory=False, encoding ='latin1')
        clean_all()
        pass
    
    def rejections():


    def make_files(self):
        clean_area()
        clean_attributes()
        clean_benefits()
        clean_network()
        clean_rules()
        clean_rates()

