
# coding: utf-8

# In[1]:

import pandas as pd
import numpy as np


# In[2]:

work_order = pd.read_excel('~/data/yarra/YVW_Work_Order_Specification_Data_1995-2014-Challenge_9934007.xlsx')


# In[3]:

water_zone = pd.read_excel("~/data/yarra/YVW_Water_Zone_Data-Challenge_9934007.xlsx")
soil = pd.read_excel("~/data/yarra/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx")
grant_code = pd.read_excel("~/data/yarra/Yarra Data/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx",sheetname="Grant Code Data")
soil_class = pd.read_excel("~/data/yarra/Yarra Data/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx",sheetname="Soil Class Data")
active = pd.read_excel("~/data/yarra/Yarra Data/YVW_Pipe_Data-Active-Challenge_9934007-v2.xlsx")
inactive = pd.read_excel("~/data/yarra/Yarra Data/YVW_Pipe_Data-Abandoned-Challenge_9934007-v2.xlsx")


# In[4]:

work_order['previous_failure_time'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['Event time'].shift()
work_order['failed'] = 1
work_order['previous_failure_cnt'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['failed'].apply(lambda x: x.cumsum())


# In[134]:

work_order.sort(['Asset ID','Event time'])


# In[6]:

active


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


# In[147]:

grant_code


# In[141]:

work_order['Event Date'].map(lambda x: x.year).value_counts()


# In[408]:

act_neigh = pd.read_csv('~/data/yarra/neighbors/active_neighbors.csv',delimiter='\t',header=None,names=['Asset ID','neighbors_act'])
act_inact_neigh = pd.read_csv('~/data/yarra/neighbors/active_inactive_neighbors.csv',delimiter='\t',header=None,names=['Asset ID','neighbors_inact'])


# In[399]:

pipe_fails = {}
for _,rec in work_order.iterrows():
    if rec['Asset ID'] in pipe_fails:
        pipe_fails[rec['Asset ID']].append(rec['Event Date'])
    else:
        pipe_fails[rec['Asset ID']]=[rec['Event Date']]


# In[402]:

pipe_types = {}
for _,rec in active.iterrows():
    if rec['Asset ID'] in pipe_types:
        pipe_types[rec['Asset ID']].append(rec['Pipe Material'])
    else:
        pipe_types[rec['Asset ID']]=[rec['Pipe Material']]


# In[420]:

act_neigh.neighbors_act = act_neigh.neighbors_act.map(lambda x: eval(x))
act_inact_neigh.neighbors_act = act_inact_neigh.neighbors_inact.map(lambda x: eval(x))


# In[432]:

set([1,3,4])
    


# In[433]:

def get_materials(x):
    d = []
    for i in x:
        d.append(pipe_types.get(i)[0])
    return list(set(d))


# In[435]:

act_neigh


# In[434]:

act_neigh['neigh_count_act'] = act_neigh.neighbors_act.map(lambda x: len(x))
act_neigh['materials_act']=act_neigh.neighbors_act.map(lambda x: get_materials(x))


# In[ ]:

act_inact_neigh['neigh_count_act'] = act_inact_neigh.neighbors_act.map(lambda x: len(x))
act_neigh['materials_inact'] = t


# In[ ]:

## Build simple dataset that attempts to predict time to failure or survival time. 

#Join work orders to inactive pipes


# In[5]:

water_zone.drop_duplicates(inplace=True)


# In[224]:

work_order['previous_failure_cnt'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['failed'].shift()
work_order['previous_failure_cnt'][work_order['Event Date']>start] = 0
work_order['previous_failure_cnt'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['previous_failure_cnt'].cumsum()
work_order.sort(['Asset ID','Event time'])


# In[366]:

## Build monthly datasets that treat every 6 month as an opportunity for failure.

import datetime
tmp_df = []

for i in range(1,4,1):
    start = datetime.datetime(2010,6,1)+datetime.timedelta(days=i*180)
    end = start+datetime.timedelta(days=180)
    work_order['previous_failure_time'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['Event time'].shift()
    work_order['failed'] = 1
    work_order['previous_failure_cnt'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['failed'].shift()
    work_order['previous_failure_cnt'][work_order['Event Date']>start] = 0
    work_order['previous_failure_cnt'] = work_order.sort(['Asset ID','Event time']).groupby(work_order['Asset ID'])['previous_failure_cnt'].cumsum()

    wrk = work_order[(work_order['Event Date']<end) & (work_order['Event Date']>start)]
    
    act = wrk.merge(active,on='Asset ID').merge(water_zone, on='Distribution Zone ID',how='left')         .merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    inact = wrk.merge(inactive,on='Asset ID').merge(water_zone, on='Distribution Zone ID',how='left')         .merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    nofail = active[active['Asset ID'].isin(act['Asset ID'].values.tolist()+inact['Asset ID'].values.tolist())==False].merge(water_zone, on='Distribution Zone ID',how='left')         .merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    tmp = pd.concat([act,inact,nofail],axis=0)
    tmp['pred_date'] = end
    tmp = tmp.merge(act_neigh,how='left',on='Asset ID').merge(act_inact_neigh,how='left',on='Asset ID')
    for 
    
    tmp_df.append(tmp)
tmp = pd.concat(tmp_df)


# In[352]:

wrk.shape


# In[365]:

pd.crosstab(tmp.pred_date,tmp.failed)


# In[342]:

tmp.pred_date.value_counts()


# In[226]:

from sklearn.preprocessing import OneHotEncoder,LabelEncoder


# In[227]:

#'Road Name',
strings = ['Comments','Distribution Zone Name\nPR - pressure reduced\nPB - pressure boosted','Grant Code',  'Grant Description','Material','Pipe Lining','Pipe Material','Road Type','Soil Class\n(see Soil Class Data tab)', 'Tapping Status','YVW Identifier'] # To Hash
cats = ['Distribution Zone ID','Post Code','Water Quality Zone ID']                                # To Encode
#['Construction Date (YYYYMMDD)','Date Insulated']


# In[228]:

mapped = {}
for s in strings:
    lb = LabelEncoder()
    lb.fit(tmp[s])
    mapped[s] = dict(zip(lb.classes_,lb.transform(lb.classes_)))


# In[357]:

get_ipython().magic(u'matplotlib inline')
import seaborn as sns

tmp.groupby('Material')['Construction Date (YYYYMMDD)'].hist()


# In[367]:

#impute missing construction dates
tmp_age = tmp[tmp['Construction Date (YYYYMMDD)'].isnull()!=True]['Construction Date (YYYYMMDD)']                 .astype(str).map(lambda x: datetime.datetime.strptime(x[:8],'%Y%m%d'))
    
tmp_age = tmp[tmp['Construction Date (YYYYMMDD)'].isnull()!=True].pred_date - tmp_age
            
mean_age = tmp_age.map(lambda x: x.days).groupby(tmp[tmp['Construction Date (YYYYMMDD)'].isnull()!=True]['Material']).mean().to_dict()
    
tmp['Construction Date (YYYYMMDD)'][tmp['Construction Date (YYYYMMDD)'].isnull()==True]  =         (tmp[tmp['Construction Date (YYYYMMDD)'].isnull()==True].pred_date -         tmp[tmp['Construction Date (YYYYMMDD)'].isnull()==True]['Material'].map(mean_age)         .map(lambda x: datetime.timedelta(days=x) if not np.isnan(x)  else 0)).map(lambda x: str(x).replace("-",'')[:8])


# In[368]:

# Calculate pipe age
tmp['pipe_age'] = (tmp.pred_date - tmp['Construction Date (YYYYMMDD)'].astype(str)  .map(lambda x: datetime.datetime.strptime(x[:8],'%Y%m%d'))).map(lambda x: x.days)


# In[369]:

# Generate Dataset for Modeling
m = tmp.pred_date.max()
tmp['holdout'] = tmp.pred_date.map(lambda x: 1 if x==m else 0 )
model = tmp[tmp['Construction Date (YYYYMMDD)']<tmp['pred_date']]         [['Nominal Size (mm)','Pipe Inside Diameter (mm)','Pipe Length (m)','failed','previous_failure_cnt','Asset ID','holdout']]

temp = tmp[tmp['Construction Date (YYYYMMDD)']<tmp['pred_date']][mapped.keys()] 

cols = []
for k,v in mapped.iteritems():
    temp[k] = temp[k].map(v)
    vals = temp[k].unique()
    for k1,v1 in v.iteritems():
        if v1 in vals:
            cols.append(k+'_'+str(k1))
enc = OneHotEncoder(handle_unknown=True,sparse=False)
temp  = pd.DataFrame(enc.fit_transform(temp),columns=cols)


# In[370]:

model.reset_index(drop=True,inplace=True)


# In[371]:

temp.reset_index(drop=True,inplace=True)


# In[372]:

modeling = pd.concat([temp,model],axis=1)


# In[373]:

modeling.fillna(0,inplace=True)
modeling['random'] = np.random.uniform(size=modeling.shape[0])


# In[374]:

modeling['Pipe Inside Diameter (mm)'][modeling['Pipe Inside Diameter (mm)']=='UNK']='0'
modeling['Pipe Inside Diameter (mm)'] = modeling['Pipe Inside Diameter (mm)'].astype(int)


# In[188]:

from sklearn.ensemble import RandomForestClassifier,RandomTreesEmbedding


# In[375]:

clf = RandomForestClassifier(n_jobs=4)


# In[376]:

clf.fit(modeling[modeling.holdout==0].drop(['failed','random','Asset ID','holdout'],axis=1),modeling[modeling.holdout==0].failed)


# In[377]:

pred = pd.DataFrame(clf.predict_proba(modeling[modeling.holdout==1].drop(['failed','random','Asset ID','holdout'],axis=1))[:,1],columns=['prediction'])
pred['predicted'] = 0
pred['predicted'][pred.prediction>.2] = 1


# In[382]:

pd.crosstab(modeling.holdout,modeling.failed)


# In[379]:

from sklearn.metrics import confusion_matrix, classification_report

print pd.crosstab(pred.prediction,modeling[modeling.holdout==1].reset_index().failed).head()
print ''
print confusion_matrix(modeling[modeling.holdout==1].failed,pred.predicted)
print ''
print classification_report(modeling[modeling.holdout==1].failed,pred.predicted)


# In[380]:

feat = zip(modeling[modeling.random<.5].drop(['failed','random','Asset ID'],axis=1).columns,clf.feature_importances_)
v = sorted(feat,key=lambda x:x[1],reverse=True)[:20]


# In[381]:

v


# In[201]:

import sqlalchemy as sa
conn = sa.engine.create_engine("postgresql://postgres@192.168.1.104/propdata")


# In[207]:

p.columns


# In[213]:

import re
p = modeling[[i[0] for i in v]+['Asset ID']]
p.columns = [re.sub('[^A-z0-9]','',i) for i in p.columns]
p.to_sql('model_features',conn,'yarra',if_exists='replace')


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




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



