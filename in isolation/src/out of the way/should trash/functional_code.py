import pandas as pd 
import numpy as np 


def determine_area(zipcode):
    area = pd.read_csv('data/clean_files/area_FL.csv')
    area.dropna(inplace=True)  
    plans = area[area['zipcode']== zipcode]
    area_key = fred[['IssuerId','ServiceAreaId']]
    area_key.reset_index()
    area_key['IssuerId']=area_key['IssuerId'].astype(int)
    return area_key

def attr_by_area(df):
    attr = pd.read_csv('data/clean_files/attr_FL.csv')
    keys = list(areas.columns.values)
    i1 = attr.set_index(keys).index
    i2 = areas.set_index(keys).index
    attr_clean = attr[~i1.isin(i2)]
    return attr_clean




if __name__ == "__main__":
    areas=determine_area(zipcode)
    attributes=attr_by_area(df)
    