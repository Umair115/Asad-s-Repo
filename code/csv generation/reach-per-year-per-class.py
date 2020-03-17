ROOT_DIR = '../../'
import sys
sys.path.append("../utils")
import tb_common
import numpy as np
import pandas as pd
import datetime
import multiprocessing
import itertools
import pytricia
import operator


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


t1_ases = tb_common.load_t1_ases()
customer_cones = tb_common.load_customer_cones()
asn2pfx = tb_common.load_asn2pfx()
as_relations = tb_common.load_as_relations()


def calculate_number_of_addresses(pfx_len):
    return 2 ** (32 - pfx_len) - 2

def get_number_IPs_from_prefixes(pfx_list):
    pyt = pytricia.PyTricia(32)
    
    for pfx in pfx_list:
        pyt[pfx] = None
        
    return get_number_IPs_from_pyt(pyt)

def get_number_IPs_from_pyt(pyt):
    
    number_IPs = 0
    
    for pfx in pyt.keys():
        # If a prefix is covered (i.e. has a parent), only consider the covering
        # prefix to correctly compute the number of reachable IPs.
        # Considering all uncovered prefixes should be enough to calculate reachability.
        if pyt.parent(pfx):
            continue
        else:
            _, pfxlen = pfx.split('/')
            number_IPs += calculate_number_of_addresses(int(pfxlen))
    
    return number_IPs


ts_merge = [
    dict(
        ts=ts,
        t1_ases=list(t1_ases.keys())[np.argmin([ abs(ts - ts2) for ts2 in t1_ases.keys() ])],
        customer_cones=list(customer_cones.keys())[np.argmin([ abs(ts - ts2) for ts2 in customer_cones.keys() ])],
        asn2pfx=list(asn2pfx.keys())[np.argmin([ abs(ts - ts2) for ts2 in asn2pfx.keys() ])],
        ixp_member_asn=list(ixp_member_asn.keys())[np.argmin([ abs(ts - ts2) for ts2 in ixp_member_asn.keys() ])],
        as_relations=list(as_relations.keys())[np.argmin([ abs(ts - ts2) for ts2 in as_relations.keys() ])]
    ) for ts in [ pd.Timestamp(np.datetime64('2006-01') + n*np.timedelta64(1, 'Y')).to_pydatetime() for n in range(14) ]
]

res_all_reach = {
    't1_reach': {ts['ts']: 0 for ts in ts_merge},
    't1_cc_reach': {ts['ts']: 0 for ts in ts_merge},
    'all_reach': {ts['ts']: 0 for ts in ts_merge}
}

for ts in ts_merge:
    prefixes = []
    for t1 in t1_ases[ts['t1_ases']]:
        prefixes += asn2pfx[ts['asn2pfx']][t1]
    
    res_all_reach['t1_reach'][ts['ts']] = get_number_IPs_from_prefixes(prefixes)

for ts in ts_merge:
    prefixes = []
    for t1 in t1_ases[ts['t1_ases']]:
        prefixes += list(itertools.chain(*[asn2pfx[ts['asn2pfx']].get(asn, []) for asn in customer_cones[ts['customer_cones']][t1]]))
    
    res_all_reach['t1_cc_reach'][ts['ts']] = get_number_IPs_from_prefixes(prefixes)

for ts in ts_merge:
    asns = list(itertools.chain(*customer_cones[ts['customer_cones']].values()))
    prefixes = list(itertools.chain(*[asn2pfx[ts['asn2pfx']].get(asn, []) for asn in asns]))
    res_all_reach['all_reach'][ts['ts']] = get_number_IPs_from_prefixes(prefixes)

df = pd.DataFrame(res_all_reach)

to_export = df.copy()
to_export.index = [ v.strftime('%Y') for v in to_export.index ]
to_export.to_csv('../../csvs/reach-per-year-per-class.csv', index_label='ts')