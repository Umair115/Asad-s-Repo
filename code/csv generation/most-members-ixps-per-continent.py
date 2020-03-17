ROOT_DIR = '../../'
import sys
sys.path.append("../../code/utils")
import tb_common
import pandas as pd
import datetime


pdb_ixps, ixp_member_asn = tb_common.load_peeringdb()


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



df = pd.DataFrame([
    {'date': ts, 'ixp': ixp, 'member_asn': asn}
    for ts, ixps in ixp_member_asn.items()
    for ixp, members in ixps.items()
    for asn in members
])

df2 = pd.DataFrame([
    {'date': ts, 'ixp': ixp, 'continent': props['region_continent']}
    for ts, ixps in pdb_ixps.items()
    for ixp, props in ixps.items()
])

df = df.merge(df2)


gb = df.groupby(['date', 'continent', 'ixp']).agg({'member_asn': 'count'}).reset_index()
gb = gb.groupby(['date', 'continent']).apply(lambda g: g.nlargest(5, 'member_asn'))

gb.to_excel('../../xlsx/most-members-ixps-per-continent.xlsx')

