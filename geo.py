import pandas as pd

'''
import active and inactive pipes geoms into postgis
should get close to this
In [31]: len(active)
Out[31]: 190632

In [32]: len(inactive)
Out[32]: 14700
'''

active = pd.read_excel("~/data/yarra/YVW_Pipe_Data-Active-Challenge_9934007-v2.xlsx")
# inactive = pd.read_excel("~/data/yarra/YVW_Pipe_Data-Abandoned-Challenge_9934007-v2.xlsx")

i = 0
# for _, rec in inactive.iterrows():
for _, rec in active.iterrows():
    print "insert into geo.active(distribution_id, asset_id, geom) values ({}, {}, st_geomfromtext('LINESTRING({} {}, {} {})'));"\
            .format(
                    rec['Distribution Zone ID'],
                    rec['Asset ID'],
                    rec['Latitude Start of Pipe'],
                    rec['Longitude Start of Pipe'],
                    rec['Lattitude End of Pipe'],
                    rec['Longitude End of Pipe']
                )
