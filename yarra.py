
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np


# In[2]:

work_order = pd.read_excel('/Users/mhabiger/Desktop/Yarra Data/YVW_Work_Order_Specification_Data_1995-2014-Challenge_9934007.xlsx')


# In[145]:

water_zone = pd.read_excel("/Users/mhabiger/Desktop/Yarra Data/YVW_Water_Zone_Data-Challenge_9934007.xlsx")
soil = pd.read_excel("/Users/mhabiger/Desktop/Yarra Data/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx")
grant_code = pd.read_excel("/Users/mhabiger/Desktop/Yarra Data/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx",sheetname="Grant Code Data")
soil_class = pd.read_excel("/Users/mhabiger/Desktop/Yarra Data/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx",sheetname="Soil Class Data")
active = pd.read_excel("/Users/mhabiger/Desktop/Yarra Data/YVW_Pipe_Data-Active-Challenge_9934007-v2.xlsx")
inactive = pd.read_excel("/Users/mhabiger/Desktop/Yarra Data/YVW_Pipe_Data-Abandoned-Challenge_9934007-v2.xlsx")


# In[132]:

work_order['previous_failure_time'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['Event time'].shift()
work_order['failed'] = 1
work_order['previous_failure_cnt'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['failed'].apply(lambda x: x.cumsum())


# In[134]:

work_order.sort(['Asset ID','Event time'])


# In[6]:

active


# In[293]:

rec


# In[301]:

print active['Latitude Start of Pipe'].min()
print active['Latitude Start of Pipe'].max()
print active['Longitude Start of Pipe'].min()
print active['Longitude Start of Pipe'].max()


# In[303]:

import folium
osm = folium.Map((-37.6542, 145.15))

lat1, lng1 = (-37.5642, 145.07)
lat2, lng2 = (-37.6042, 145.10)
n = 0 
for rec in active[(lat1>active['Latitude Start of Pipe'])& (lat2<active['Latitude Start of Pipe'])&                   (lng1<active['Longitude Start of Pipe']) & (lng2>active['Longitude Start of Pipe'])].iterrows():
    rec= rec[1]
    folium.PolyLine([[rec['Latitude Start of Pipe'],rec['Longitude Start of Pipe']],[rec['Lattitude End of Pipe'],rec['Longitude End of Pipe']]]).add_to(osm)

for rec in inactive[(lat1>inactive['Latitude Start of Pipe'])& (lat2<inactive['Latitude Start of Pipe'])&                   (lng1<inactive['Longitude Start of Pipe']) & (lng2>inactive['Longitude Start of Pipe'])].iterrows():
    rec= rec[1]
    folium.PolyLine([[rec['Latitude Start of Pipe'],rec['Longitude Start of Pipe']],[rec['Lattitude End of Pipe'],rec['Longitude End of Pipe']]],color='red').add_to(osm)
    
osm


# In[7]:

inactive


# In[197]:

water_zone


# In[200]:

soil['Asset ID'].shape


# In[165]:

soil.columns


# In[146]:

soil_class


# In[147]:

grant_code


# In[141]:

work_order['Event Date'].map(lambda x: x.year).value_counts()


# In[ ]:

## Build simple dataset that attempts to predict time to failure or survival time. 

#Join work orders to inactive pipes


# In[206]:

water_zone.drop_duplicates(inplace=True)


# In[219]:

## Build monthly datasets that treat every 6 month as an opportunity for failure.

import datetime
tmp_df = []
start = datetime.datetime(2011,1,1)
for i in range(1,2,1):

    end = start+datetime.timedelta(days=i*180)
    wrk = work_order[(work_order['Event Date']<end) & (work_order['Event Date']>start)]
    
    act = wrk.merge(active,on='Asset ID').merge(water_zone, on='Distribution Zone ID',how='left')         .merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    inact = wrk.merge(inactive,on='Asset ID').merge(water_zone, on='Distribution Zone ID',how='left')         .merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    nofail = active[active['Asset ID'].isin(act['Asset ID'].values.tolist()+inact['Asset ID'].values.tolist())==False].merge(water_zone, on='Distribution Zone ID',how='left')         .merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    tmp = pd.concat([act,inact,nofail],axis=0)
    tmp['pred_date'] = end
    tmp_df.append(tmp)


# In[260]:

tmp


# In[243]:

from sklearn.preprocessing import OneHotEncoder,LabelEncoder


# In[308]:

tmp['Pipe Shape'].value_counts()


# In[ ]:

['City','Comments','Description','Distribution Zone Name\nPR - pressure reduced\nPB - pressure boosted','Grant Code',  'Grant Description','Material','Pipe Lining','Pipe Material'] # To Hash
['Distribution Zone ID']                                # To Encode
['Construction Date (YYYYMMDD)','Date Insulated']


# In[244]:

tmp.columns


# In[242]:

DictVectorizer()


# In[240]:

tmp['Soil Description'].value_counts()


# In[238]:

hasher = FeatureHasher(input_type='string')
#hasher.fit(tmp[['Soil Description','Tapping Status']])
hasher.transform(tmp[['Soil Description']])


# In[236]:

tmp[['Soil Description','Tapping Status']].shape


# In[ ]:




# In[ ]:




# In[181]:

active[active['Asset ID'].isin(act['Asset ID'].values.tolist()+inact['Asset ID'].values.tolist())==False].shape


# In[86]:

"""
Code for pulling down weather data for all stations in Victoria state/province
"""

import requests
import bs4
import requests, zipfile, StringIO
import time
obs_types = {'136':'rainfall',
            '122':'max_temp','123':'min_temp','193':'solar'}
for key,val in obs_types.iteritems():
    dat = pd.read_excel('/users/mhabiger/Desktop/Yarra Data/weatherlookup.xlsx',sheetname=val)
    for item in dat[(dat.EndYear>2000) & (dat.StartYear<2000)].iterrows():
        site = """http://www.bom.gov.au"""
        pth = """http://www.bom.gov.au/jsp/ncc/cdio/weatherData/av?p_nccObsCode=%s&p_display_type=dailyDataFile&p_stn_num=%s&p_startYear=""" % (key,item[1].Site)
        pg = requests.get(pth)
        if pg.status_code!=200:
            break
        tmp = bs4.BeautifulSoup(pg.text)
        for i in tmp.find_all('a'):
            if i.text=='All years of data':
                dwnl = i.get('href')
                v = requests.get(site+dwnl,stream=True)
                z = zipfile.ZipFile(StringIO.StringIO(v.content))
                p = z.filelist[0]
                ex_pth = '/users/mhabiger/Desktop/Yarra Data/'+val+'/'
                z.extract(p,path=ex_pth)
                time.sleep(5)
        if pg.status_code!=200:
            break
    if pg.status_code!=200:
        break


# In[37]:




# In[46]:




# In[51]:

z.extract(p)


# In[ ]:



