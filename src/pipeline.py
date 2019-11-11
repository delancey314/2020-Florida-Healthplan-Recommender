import pandas as pd 
import numpy as np 


#this gets rid of the Dental Only Plans leaving medical insurance plans then drops the column as superfilous
area=area[area.DentalOnlyPlan != 'Yes']
area.drop('DentalOnlyPlan', axis=1, inplace=True)

area=area.drop(['BusinessYear','SourceName', 'ImportDate'], axis = 1)
area.info()

class Pipeline(self):
    

    def __init__(self, *args, **kwargs):
        self.fl= True
        self.load_file=None
        self.workfile=None
        self.file_list = ['area',]
#attr=('../data/Plan_Attributes_PUF.csv',low_memory=False, encoding ='latin1')
#rate=('../data/Rate_PUF.csv',low_memory=False, encoding ='latin1')
area=('../data/raw_files/Service_Area_PUF.csv',low_memory=False, encoding ='latin1') }
        super().__init__(*args, **kwargs)
        }

     def make_files(self):
         clean_area()



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

