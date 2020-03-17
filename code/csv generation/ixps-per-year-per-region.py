
# coding: utf-8

# In[9]:


ROOT_DIR = '../../'
import sys
sys.path.append("../../code/utils")
import tb_common
import pandas as pd
import datetime


# In[10]:


pdb_ixps, ixp_member_asn = tb_common.load_peeringdb()


# In[11]:


pdb_dates = [datetime.datetime(2008, 2, 15, 23, 56, 33),
 datetime.datetime(2009, 10, 14, 16, 59, 58),
 datetime.datetime(2010, 7, 29, 0, 0),
 datetime.datetime(2011, 1, 1, 0, 0),
 datetime.datetime(2012, 1, 1, 0, 0),
 datetime.datetime(2013, 1, 1, 0, 0),
 datetime.datetime(2014, 1, 1, 0, 0),
 datetime.datetime(2015, 1, 1, 0, 0),
 datetime.datetime(2016, 1, 1, 0, 0),
 datetime.datetime(2017, 1, 31, 0, 0),
 datetime.datetime(2018, 1, 31, 0, 0),
 datetime.datetime(2019, 1, 31, 0, 0)]

for date in list(pdb_ixps.keys()):
    if date not in pdb_dates:
        del(pdb_ixps[date])
        del(ixp_member_asn[date])


# In[12]:


tmp = [ {'ts': ts, 'region_continent': ixp_data['region_continent']} for ts, ixps in pdb_ixps.items() for ixp_data in ixps.values() ]
df = pd.DataFrame(tmp)

gb = df.groupby(['ts'])['region_continent'].value_counts().unstack()

gb = gb.fillna(0)
gb.index = [ v.strftime('%Y') for v in gb.index ]
gb.to_csv('../../csvs/ixps-per-year-per-region.csv', index_label='ts')

