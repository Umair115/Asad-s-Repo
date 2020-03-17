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


def calc_IXP_reachability(ts):
    # ts, tqdm_pos = params
    
    ixp_customer_cone_asn = {}

    for ixp, member_asn in ixp_member_asn[ts['ixp_member_asn']].items():
        reachable_asn = []
        for asn in member_asn:
            if asn in t1_ases[ts['t1_ases']]:
                continue
            else:
                reachable_asn += customer_cones[ts['customer_cones']].get(asn, [])
            #reachable_asn += customer_cones[ts['customer_cones']].get(asn, [])
        ixp_customer_cone_asn[ixp] = set(reachable_asn)
        
    ixp_prefixes = { ixp: minimise_pfx_list(set(itertools.chain( *[ asn2pfx[ts['asn2pfx']].get(asn, []) for asn in ixp_asn ]))) for ixp, ixp_asn in ixp_customer_cone_asn.items() }

    number_ips_reached = 0
    pyt = pytricia.PyTricia(32)

    res = []

    for _ in range(len(ixp_prefixes)):
    #for _ in tqdm.tnrange(len(ixp_prefixes), desc=str(ts['ts'].year), position=tqdm_pos):
        tmp = [ (ixp, get_number_IPs_from_prefixes(pfx_list)) for ixp, pfx_list in ixp_prefixes.items() ]

        ixp, reach = zip(*tmp)
        next_ixp = ixp[np.argmax(reach)]
        next_reach = np.max(reach)

        next_reach = get_additional_IXP_reach(pyt, ixp_prefixes[next_ixp], number_ips_reached)

        tmp = [ entry[0] for entry in tmp if entry[1] >= next_reach ]
        tmp = [ (ixp, get_additional_IXP_reach(pyt, ixp_prefixes[ixp], number_ips_reached)) for ixp in tmp]

        ixp, reach = zip(*tmp)
        next_ixp = ixp[np.argmax(reach)]
        next_reach = np.max(reach)
        number_ips_reached += next_reach

        for pfx in ixp_prefixes[next_ixp]:
            pyt[pfx] = None

        res.append((next_ixp, next_reach))
        del(ixp_prefixes[next_ixp])

        ixp_prefixes = { ixp: remove_pfxes(pyt, pfx_list) for ixp, pfx_list in ixp_prefixes.items() }
        
    return (ts['ts'], res)

def remove_pfxes(pyt, pfx_list):
    return set([pfx for pfx in pfx_list if not pyt.get_key(pfx)])

def minimise_pfx_list(pfx_list):
    pyt = pytricia.PyTricia(32)
    
    for pfx in pfx_list:
        pyt[pfx] = None
        
    pyt = minimise_pyt(pyt)
    
    return set(pyt.keys())

def minimise_pyt(pyt):
    pyt_new = pytricia.PyTricia(32)
    
    for pfx in pyt.keys():
        if not pyt.parent(pfx):
            pyt_new[pfx] = None
            
    return pyt_new

def get_additional_IXP_reach(pyt, ixp_pfx, number_ips_reached):
    
    if len(ixp_pfx) == 0:
        return 0
    
    new_pfx = [pfx for pfx in ixp_pfx if pfx not in pyt]
    
    for pfx in new_pfx:
        pyt[pfx] = None
        
    new_reach = get_number_IPs_from_pyt(pyt)
        
    for pfx in new_pfx:
        del(pyt[pfx])
        
    return new_reach - number_ips_reached

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

pool = multiprocessing.Pool()
res_ixp_without_t1 = dict(pool.map(calc_IXP_reachability, ts_merge))
pool.terminate()

def get_IXP_reach(ixp, ts):
    reachable_asn = []
    for asn in ixp_member_asn[ts['ixp_member_asn']][ixp]:
        if asn in t1_ases[ts['t1_ases']]:
            continue
        else:
            reachable_asn += customer_cones[ts['customer_cones']].get(asn, [])
            
    pfxes = minimise_pfx_list(set(itertools.chain( *[ asn2pfx[ts['asn2pfx']].get(asn, []) for asn in reachable_asn] )))
    
    return get_number_IPs_from_prefixes(pfxes)

def do_calc(ts):
    v = [ (additional_reach, get_IXP_reach(ixp, ts)) for (ixp, additional_reach) in res_ixp_without_t1[ts['ts']] ]
    return (ts['ts'], v)

pool = multiprocessing.Pool()
res_reach_redundancy = dict(pool.map(do_calc, ts_merge))
pool.terminate()

sr_list = []
for ts, x in sorted(res_reach_redundancy.items()):
    y = np.cumsum(x, axis=0)
    sr_list.append(pd.Series(zip(*y)[0], name='%s - reachable' % ts.strftime('%Y')))
    sr_list.append(pd.Series(zip(*y)[1], name='%s - redundant' % ts.strftime('%Y')))

df = pd.DataFrame(sr_list).transpose()
df.index = df.index + 1
display(df.head())
df.to_csv('../csvs/ixps-reachability-vs-redundancy.csv')

sr_list = []
for ts, x in sorted(res_reach_redundancy.items()):
    y = np.cumsum(x, axis=0)
    s1 = pd.Series(zip(*y)[0], name='%s - reachable' % ts.strftime('%Y'))
    s2 = pd.Series(zip(*y)[1]) / s1
    s2.name = '%s - redundant' % ts.strftime('%Y')
    sr_list.extend([s1, s2])

df = pd.DataFrame(sr_list).transpose()
df.index = df.index + 1
display(df.head())
df.to_csv('../csvs/ixps-reachability-vs-relative-redundancy.csv')
