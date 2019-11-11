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

     def make_files(self):
         clean_area()
         clean_attributes()



    def load(self):
        self.area=pd.read_csv('../data/raw_files/Service_Area_PUF.csv',low_memory=False, encoding ='latin1')
        self.ben_cost = pd.read_csv('../data/Benefits_Cost_Sharing_PUF.csv',low_memory=False, encoding ='latin1')
        
        pass

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

    def clean_attributes(self):
        self.workfile=pd.read_csv('../data/raw_files/Plan_Attributes_PUF.csv',low_memory=False, encoding ='latin1')
        clean_all()
         
         if self.fl != True:
            #Florida does not have any attribute exclusions. Most states do.
            attribute_exclusions()

        # no value for recommender at this time
        attr=attr.drop(['BusinessYear','SourceName', 'ImportDate',
                'PlanExpirationDate','PlanEffectiveDate',
                'URLForEnrollmentPayment', 'FormularyURL','IsGuaranteedRate'], axis=1)
        #These are either 0, N/A, or No for all values in FL
        attr=attr.drop(['IndianPlanVariationEstimatedAdvancedPaymentAmountPerEnrollee',
                'ChildOnlyPlanId','HSAOrHRAEmployerContribution', 
                'HSAOrHRAEmployerContributionAmount','PlanLevelExclusions'], axis = 1)
        #These are alterations to default rates.  rate file cannot be used at this time.

        attr=attr.drop(['MEHBInnTier1IndividualMOOP','MEHBInnTier1FamilyPerPersonMOOP',
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
        attr.drop(['MarketCoverage', 'TIN','HIOSProductId', 'HPID', 'FormularyId', 'IsNewPlan',
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
        
        self.fronters=attr[['IssuerId', 'StandardComponentId', 'PlanMarketingName', 'NetworkId',
       'ServiceAreaId','PlanId']]

        self.special=attr[['IsNoticeRequiredForPregnancy','IsReferralRequiredForSpecialist','SpecialistRequiringReferral']]
        attr.drop(['IsNoticeRequiredForPregnancy','IsReferralRequiredForSpecialist','SpecialistRequiringReferral'], axis=1, inplace=True)

        self.ld_diab_fx_mon=attr[['SBCHavingaBabyDeductible',
            'SBCHavingaBabyCopayment', 'SBCHavingaBabyCoinsurance',
            'SBCHavingaBabyLimit', 'SBCHavingDiabetesDeductible',
            'SBCHavingDiabetesCopayment', 'SBCHavingDiabetesCoinsurance',
            'SBCHavingDiabetesLimit', 'SBCHavingSimplefractureDeductible',
            'SBCHavingSimplefractureCopayment',
            'SBCHavingSimplefractureCoinsurance', 'SBCHavingSimplefractureLimit']]
        attr.drop(['SBCHavingaBabyDeductible',
            'SBCHavingaBabyCopayment', 'SBCHavingaBabyCoinsurance',
            'SBCHavingaBabyLimit', 'SBCHavingDiabetesDeductible',
            'SBCHavingDiabetesCopayment', 'SBCHavingDiabetesCoinsurance',
            'SBCHavingDiabetesLimit', 'SBCHavingSimplefractureDeductible',
            'SBCHavingSimplefractureCopayment',
            'SBCHavingSimplefractureCoinsurance', 'SBCHavingSimplefractureLimit'],axis=1,inplace=True)

        self.plan_docs=[['URLForSummaryofBenefitsCoverage','PlanBrochure']]
        attr.drop(['URLForSummaryofBenefitsCoverage','PlanBrochure'],axis=1,inplace=True)
        '''
        Issuers only have to report if they have a disease management support progam 
        and then in another column list what they are. The blanks are given a 'No' which is 
        later used for 'No management program in hotcoding
        '''
        attr.fillna('No', inplace=True)
        # duplicate name issue fix
        attr.replace(['Weight Loss Management Program','High Blood Pressure & High Cholesterol Management Program',],
        [' Weight Loss Programs',' Blood Pressure-Cholesterol Management Program'],inplace=True)

