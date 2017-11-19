
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


# In[2]:

work_order = pd.read_excel('~/data/yarra/YVW_Work_Order_Specification_Data_1995-2014-Challenge_9934007.xlsx')


# In[3]:

water_zone = pd.read_excel("~/data/yarra/YVW_Water_Zone_Data-Challenge_9934007.xlsx")
soil = pd.read_excel("~/data/yarra/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx")
grant_code = pd.read_excel("~/data/yarra/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx",sheetname="Grant Code Data")
soil_class = pd.read_excel("~/data/yarra/YVW_Pipe_Soil_Type_Data-Challenge_9934007.xlsx",sheetname="Soil Class Data")
active = pd.read_excel("~/data/yarra/YVW_Pipe_Data-Active-Challenge_9934007-v2.xlsx")
inactive = pd.read_excel("~/data/yarra/YVW_Pipe_Data-Abandoned-Challenge_9934007-v2.xlsx")


# In[2]:


work_order['previous_failure_time'] = work_order.sort_values(['Asset ID','Event time']).groupby(work_order['Asset ID'])['Event time'].shift()
work_order['failed'] = 1
work_order['previous_failure_cnt'] = work_order.sort_values(['Asset ID','Event time']).groupby(work_order['Asset ID'])['failed'].apply(lambda x: x.cumsum())


# In[3]:



act_neigh = pd.read_csv('~/data/yarra/neighbors/active_neighbors.csv',delimiter='\t',header=None,names=['Asset ID','neighbors_act'])
act_inact_neigh = pd.read_csv('~/data/yarra/neighbors/active_inactive_neighbors.csv',delimiter='\t',header=None,names=['Asset ID','neighbors_inact'])
#inact_inact_neigh = pd.read_csv('~/data/yarra/neighbors/active_inactive_neighbors.csv',delimiter='\t',header=None,names=['Asset ID','neighbors_inact'])
#inact_act_neigh = pd.read_csv('~/data/yarra/neighbors/active_inactive_neighbors.csv',delimiter='\t',header=None,names=['Asset ID','neighbors_inact'])


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

for _,rec in inactive.iterrows():
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
    return set(d)


# In[ ]:





# In[4]:


# In[434]:

act_neigh['neigh_count_act'] = act_neigh.neighbors_act.map(lambda x: len(x))
act_neigh['materials_act']=act_neigh.neighbors_act.map(lambda x: get_materials(x))


# In[ ]:

act_inact_neigh['neigh_inact_count_act'] = act_inact_neigh.neighbors_act.map(lambda x: len(x))
act_inact_neigh['materials_inact']=act_inact_neigh.neighbors_act.map(lambda x: get_materials(x))



# In[ ]:

## Build simple dataset that attempts to predict time to failure or survival time. 

#Join work orders to inactive pipes


# In[5]:

water_zone.drop_duplicates(inplace=True)

#active.['Pipe Material'].value_counts()


# In[17]:


active.head()


# In[5]:



def fuck_fails(act,inact,date):
    n = 0
    if isinstance(set(),type(act)):
        for i in act:
            ld = pipe_fails.get(i)
            if ld:
                for j in ld:
                    if j<date:
                        n+=1
    if isinstance(set(),type(inact)):
        for i in inact:
            ld = pipe_fails.get(i)
            if ld:
                for j in ld:
                    if j<date:
                        n+=1
    return n
        


# In[48]:


## Build monthly datasets that treat every 6 month as an opportunity for failure.

import datetime
tmp_df = []

for i in range(1,4,1):
    start = datetime.datetime(2010,6,1)+datetime.timedelta(days=i*180)
    end = start+datetime.timedelta(days=180)
    work_order['previous_failure_time'] = work_order.sort_values(['Asset ID','Event time']).groupby(work_order['Asset ID'])['Event time'].shift()
    work_order['failed'] = 1
    work_order['previous_failure_cnt'] = work_order.sort_values(['Asset ID','Event time']).groupby(work_order['Asset ID'])['failed'].shift()
    work_order['previous_failure_cnt'][work_order['Event Date']>start] = 0
    work_order['previous_failure_cnt'] = work_order.sort_values(['Asset ID','Event time']).groupby(work_order['Asset ID'])['previous_failure_cnt'].cumsum()

    wrk = work_order[(work_order['Event Date']<end) & (work_order['Event Date']>start)]
    
    act = wrk.merge(active,on='Asset ID').merge(water_zone, on='Distribution Zone ID',how='left').merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    inact = wrk.merge(inactive,on='Asset ID').merge(water_zone, on='Distribution Zone ID',how='left').merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    nofail = active[active['Asset ID'].isin(act['Asset ID'].values.tolist()+inact['Asset ID'].values.tolist())==False].merge(water_zone, on='Distribution Zone ID',how='left').merge(soil[soil['Asset ID'].isnull()!=True],on='Asset ID',how='left').merge(grant_code, how='left',left_on="Grant Code\n(see Grant Code Data tab)" ,right_on="Grant Code")         
        
    tmp = pd.concat([act,inact,nofail],axis=0)
    tmp['pred_date'] = end
    tmp = tmp.merge(act_neigh,how='left',on='Asset ID').merge(act_inact_neigh,how='left',on='Asset ID')
    tmp['neighbor_fails'] =  tmp[['neighbors_act','neighbors_inact','pred_date']].apply(lambda x: fuck_fails(x.neighbors_act,x.neighbors_inact,x.pred_date),axis=1)
    
    tmp_df.append(tmp)
tmp = pd.concat(tmp_df)


# In[55]:


del active, inactive, work_order, tmp_df


# In[49]:


p = tmp.materials_act.astype(str).value_counts()
material_combos = p[p>100].to_dict()


# In[50]:


tmp.materials_act=tmp.materials_act.astype(str).map(material_combos)


# In[51]:


# In[226]:

from sklearn.preprocessing import OneHotEncoder,LabelEncoder


# In[227]:

#'Road Name',
strings = ['materials_act','materials_inact','Comments','Distribution Zone Name\nPR - pressure reduced\nPB - pressure boosted','Grant Code',  'Grant Description','Material','Pipe Lining','Pipe Material','Road Type','Soil Class\n(see Soil Class Data tab)', 'Tapping Status','YVW Identifier'] # To Hash
cats = ['Distribution Zone ID','Post Code','Water Quality Zone ID']                                # To Encode
#['Construction Date (YYYYMMDD)','Date Insulated']


# In[228]:

mapped = {}
for s in strings:
    lb = LabelEncoder()
    lb.fit(tmp[s].astype(str))
    mapped[s] = dict(zip(lb.classes_,lb.transform(lb.classes_.astype(str))))


# In[52]:


#impute missing construction dates
tmp_age = tmp[tmp['Construction Date (YYYYMMDD)'].isnull()!=True]['Construction Date (YYYYMMDD)'].astype(str).map(lambda x: datetime.datetime.strptime(x[:8],'%Y%m%d'))
    
tmp_age = tmp[tmp['Construction Date (YYYYMMDD)'].isnull()!=True].pred_date - tmp_age
            
mean_age = tmp_age.map(lambda x: x.days).groupby(tmp[tmp['Construction Date (YYYYMMDD)'].isnull()!=True]['Material']).mean().to_dict()
    
tmp['Construction Date (YYYYMMDD)'][tmp['Construction Date (YYYYMMDD)'].isnull()==True] = (tmp[tmp['Construction Date (YYYYMMDD)'].isnull()==True].pred_date - tmp[tmp['Construction Date (YYYYMMDD)'].isnull()==True]['Material'].map(mean_age).map(lambda x: datetime.timedelta(days=x) if not np.isnan(x)  else datetime.timedelta(days=0))).map(lambda x: str(x).replace("-",'')[:8])



# In[ ]:



#p['ind_len'] = (tmp.materials_act.astype(str).value_counts()).index.str.split(",").map(lambda x:len(x))


# In[37]:


for k,v in mapped.iteritems():
    print k, len(v)


# In[53]:


# Calculate pipe age
tmp['pipe_age'] = (tmp.pred_date - tmp['Construction Date (YYYYMMDD)'].astype(str)  .map(lambda x: datetime.datetime.strptime(x[:8],'%Y%m%d'))).map(lambda x: x.days)


real_feat = ['pipe_age','neighbor_fails','neigh_count_act','neigh_inact_count_act','Nominal Size (mm)','Pipe Inside Diameter (mm)','Pipe Length (m)','failed','previous_failure_cnt','Asset ID','holdout']

# Generate Dataset for Modeling
m = tmp.pred_date.max()
tmp['holdout'] = tmp.pred_date.map(lambda x: 1 if x==m else 0 )
model = tmp[tmp['Construction Date (YYYYMMDD)']<tmp['pred_date']][real_feat]

temp = tmp[tmp['Construction Date (YYYYMMDD)']<tmp['pred_date']][mapped.keys()] 

cols = []
for k,v in mapped.iteritems():
    temp[k] = temp[k].astype(str).map(v)
    vals = temp[k].unique()
    for k1,v1 in v.iteritems():
        if v1 in vals:
            cols.append(k+'_'+str(k1))
enc = OneHotEncoder(handle_unknown=True,sparse=False)
temp  = pd.DataFrame(enc.fit_transform(temp.fillna(0)),columns=cols)



# In[56]:


del tmp


# In[54]:


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


# In[72]:


# In[188]:

from sklearn.ensemble import RandomForestClassifier


# In[375]:

clf = RandomForestClassifier(n_jobs=8, n_estimators=1000)


# In[376]:

clf.fit(modeling[modeling.holdout==0].drop(['failed','random','Asset ID','holdout'],axis=1),modeling[modeling.holdout==0].failed)




# In[73]:


# In[377]:
prob = .45
pred = pd.DataFrame(clf.predict_proba(modeling[modeling.holdout==1].drop(['failed','random','Asset ID','holdout'],axis=1))[:,1],columns=['prediction'])
pred['predicted'] = 0
pred['predicted'][pred.prediction>prob] = 1


# In[382]:

pd.crosstab(modeling.holdout,modeling.failed)


# In[379]:

from sklearn.metrics import confusion_matrix, classification_report
print "prob>{}".format(prob)
print pd.crosstab(pred.prediction,modeling[modeling.holdout==1].reset_index().failed).head()
print ''
print confusion_matrix(modeling[modeling.holdout==1].failed,pred.predicted)
print ''
print classification_report(modeling[modeling.holdout==1].failed,pred.predicted)


# In[380]:

feat = zip(modeling[modeling.random<.5].drop(['failed','random','Asset ID'],axis=1).columns,clf.feature_importances_)
v = sorted(feat,key=lambda x:x[1],reverse=True)[:20]


# In[75]:


modeling.to_json('modeling.json')


# In[ ]:


prob>.7
failed         0.0  1.0
prediction             
0.00        166672  742
0.01          8674   87
0.02          1859   19
0.03           711    5
0.04           396    5

[[180525     48]
 [  1206    573]]

             precision    recall  f1-score   support

        0.0       0.99      1.00      1.00    180573
        1.0       0.92      0.32      0.48      1779

avg / total       0.99      0.99      0.99    182352

Out[59]:
[('previous_failure_cnt', 0.24476831077950578),
 ('pipe_age', 0.22043142419305409),
 (u'Pipe Length (m)', 0.13407322802456217),
 ('neigh_count_act', 0.06098408030744587),
 ('neighbor_fails', 0.040205090114620923),
 ('materials_act_312.0', 0.019067729728191336),
 (u'Pipe Inside Diameter (mm)', 0.013000735959672908),
 (u'Nominal Size (mm)', 0.0121704668686079),
 ('Road Type_TOR', 0.0077998932935852468),
 ('Distribution Zone Name\nPR - pressure reduced\nPB - pressure boosted_Somerton Res',
  0.0076200452702319876),
 ('YVW Identifier_WDZ161', 0.0073138860090715451),
 ('Road Type_CLOSE', 0.0069238879985544554),
 ('neigh_inact_count_act', 0.0055844476037052813),
 ('Material_nan', 0.0053446041652595363),
 ('Road Type_LINK', 0.0052731464031005038),
 ('Grant Code_34.002-54', 0.0052649271857429106),
 ('Pipe Material_UNKNOWN', 0.0051340777691720576),
 ('materials_act_759.0', 0.0047057417901376726),
 ('Road Type_GLADE', 0.0040750464100155924),
 ('Road Type_VIEW', 0.003763615980879269)]


# In[ ]:


failed         0.0  1.0
prediction             
0.000000    178081  935
0.012500         1    0
0.014286         1    0
0.016667         2    0
0.020000        10    0

[[179307   1266]
 [   965    814]]

             precision    recall  f1-score   support

        0.0       0.99      0.99      0.99    180573
        1.0       0.39      0.46      0.42      1779

avg / total       0.99      0.99      0.99    182352

Out[136]:
[(u'Pipe Length (m)', 0.45489018199936576),
 ('previous_failure_cnt', 0.37962207170790929),
 (u'Nominal Size (mm)', 0.010699827569877498),
 (u'Pipe Inside Diameter (mm)', 0.01041831640946903),
 ('Road Type_TOR', 0.0091559558089513819),
 ('Road Type_LINK', 0.0077414643919641061),
 ('Road Type_CLOSE', 0.0076772134719318933),
 ('Grant Code_34.002-54', 0.0053883430705394929),
 ('Road Type_GLADE', 0.0043475965778093529),
 ('Road Type_VIEWS', 0.004220701492931531),
 ('Grant Code_52.010-00/3', 0.0040173222920212416),
 ('Road Type_VIEW', 0.0035248584815778293),
 ('Road Type_GATE', 0.0034348948587974242),
 ('Grant Code_51.007-12', 0.0033582070581607685),
 ('Grant Code_34.001-44/3', 0.0020835917893530804),
 ('Road Type_GATEWAY', 0.0018871414899018612),
 ('Road Type_GROVE', 0.0018645634202226848),
 ('Distribution Zone Name\nPR - pressure reduced\nPB - pressure boosted_Somerton Res',
  0.0016665283625776459),
 ('YVW Identifier_WDZ161', 0.0016406049322309202),
 ('Pipe Material_UNKNOWN', 0.001570033115845422)]


# In[ ]:


prob>0.55
500 trees

failed         0.0  1.0
prediction             
0.000       146032  499
0.002        17642  164
0.004         5888   80
0.006         2883   37
0.008         1626   22

[[180195    378]
 [   953    826]]

             precision    recall  f1-score   support

        0.0       0.99      1.00      1.00    180573
        1.0       0.69      0.46      0.55      1779

avg / total       0.99      0.99      0.99    182352

