from tb_common import ROOT_DIR

import bz2
import datetime
import glob
import gzip
import json
import multiprocessing
import re

import pandas as pd

from collections import defaultdict

def load_t1_ases():
	t1_ases = {}

	for f in glob.glob(ROOT_DIR + 'data/caida/cc*.txt'):
		with open(f) as cc:
			ts = re.findall(r'cc(\d{8}).txt', f)[0]
			ts = datetime.datetime.strptime(ts, '%Y%m%d')
	
			clique = cc.readlines()[0]
			clique = clique[len('# inferred clique: '):]
			clique = map(int, clique.split())
	
			t1_ases[ts] = clique

	return t1_ases

def parse_customer_cone_file(f):
	res = {}
	with bz2.open(f, 'rt') as cc:
		file_info = re.search(r'(?P<date>\d{8})\.ppdc-ases\.txt\.bz2$', f).groupdict()
		ts = file_info['date']
		ts = datetime.datetime.strptime(ts, '%Y%m%d')
	
		for line in cc:
			if line.startswith('#'):
				continue
			line = list(map(int, line.split()))
			res[line[0]] = line[1:]

		return (ts, res)

def load_customer_cones():
	pool = multiprocessing.Pool()
	customer_cones = dict(pool.map(
		parse_customer_cone_file,
		glob.glob(ROOT_DIR + 'data/caida/cc/*.ppdc-ases.txt.bz2')
	))
	pool.close()

	return customer_cones

def parse_asn2pfx_file(f, debug_print=False):
	res = defaultdict(list)
	unparseable_asn = []
	parseable_pfx = []
	unparseable_pfx = []
	
	with gzip.open(f, 'rt') as pfx:
		ts = re.search(r'routeviews-(?P<collector>[a-zA-Z0-9]{3})-(?P<timestamp>\d{8}-\d{4}).pfx2as.gz', f)
		ts = ts.group("timestamp")
		ts = datetime.datetime.strptime(ts, '%Y%m%d-%H%M')
		#print ts
		
		for i, line in enumerate(pfx):
			line = line.split()
			pfx = '%s/%s' % (line[0], line[1])
			
			# ASN part might contain '_' and ',' to separate ASNs
			# Unify them to contain ',' all the time for now
			asn = line[2]
			asn = asn.replace('_', ',')
			asn_split = asn.split(',')
			
			for asn in asn_split:
					
				# Try to convert ASN to int as sanitiy check
				try:
					asn = int(asn)
					res[asn].append(pfx)
					parseable_pfx.append(pfx)
				except ValueError:
					unparseable_asn.append(asn)
					unparseable_pfx.append(pfx)
	if debug_print:	
		print(ts, len(unparseable_asn), i, 100.*len(unparseable_asn)/i)
	return (ts, dict(res))
	

def load_asn2pfx():
	pool = multiprocessing.Pool()
	asn2pfx = dict(pool.map(
		parse_asn2pfx_file,
		glob.glob(ROOT_DIR + 'data/caida/pfx2as/routeviews*.pfx2as.gz'),
	))
	pool.close()

	return asn2pfx

def load_peeringdb():
	ixp_member_asn = {}
	pdb_ixps = {}

	for f in sorted(glob.glob(ROOT_DIR + 'data/peeringdb/json_dumps/peeringdb*.json.bz2')):
		if 'peeringdb_dump' in f:
			ts = re.search(r'peeringdb_dump_(?P<timestamp>\d{4}_\d{2}_\d{2}).json.bz2', f)
			ts = ts.group('timestamp')
			ts = datetime.datetime.strptime(ts, '%Y_%m_%d')
		else:
			ts = re.search(r'peeringdb.(?P<timestamp>\d{10}).json.bz2', f)
			ts = ts.group('timestamp')
			ts = datetime.datetime.fromtimestamp(int(ts))
			
		print(f, ts)
		
		with bz2.open(f, 'rt') as dump:
			pdb = json.load(dump)
			df_peerParticipantsPublics = pd.DataFrame(pdb['peerParticipantsPublics'])
			#print len(df_peerParticipantsPublics[df_peerParticipantsPublics.local_asn.isna()])
			df_peerParticipantsPublics.dropna(axis='index', subset=['local_asn'], inplace=True)
			df_peerParticipantsPublics.local_asn = df_peerParticipantsPublics.local_asn.astype(int)
			gb = df_peerParticipantsPublics.groupby(['public_id']).agg({'local_asn': 'unique'})
			gb = gb.reset_index()
			gb = gb.merge(pd.DataFrame(pdb['mgmtPublics']).loc[:, ['id', 'name']], left_on='public_id', right_on='id')
			gb = gb.loc[:, ['local_asn', 'name']]
			gb = gb.set_index('name')
			ixp_member_asn[ts] = gb['local_asn'].to_dict()
			
			df_mgmtPublics = pd.DataFrame(pdb['mgmtPublics'])
			pdb_ixps[ts] = df_mgmtPublics.loc[:, ['name', 'country', 'region_continent']].set_index('name').to_dict(orient='index')

	return pdb_ixps, ixp_member_asn

def load_autnums():
	with bz2.open(ROOT_DIR + 'data/autnums/autnums.1516114039.parsed.json.bz2', 'rt') as f:
		autnums = {int(asn): name for asn, name in json.load(f).items()}
	return autnums

def parse_as_relation_file(f):
	res = {}
	with bz2.open(f, 'rt') as asrel:
		file_info = re.search(r'(?P<date>\d{8})\.as-rel\.txt\.bz2$', f).groupdict()
		ts = file_info['date']
		ts = datetime.datetime.strptime(ts, '%Y%m%d')
		
		for line in asrel:
			if line.startswith('#'):
				continue
			line = list(map(int, line.split('|')))
			res[(line[0], line[1])] = line[2]

		return (ts, res)
	

def load_as_relations():
	pool = multiprocessing.Pool()
	as_relations = dict(pool.map(
		parse_as_relation_file,
		glob.glob(ROOT_DIR + 'data/caida/as-relationships/*.as-rel.txt.bz2')
	))
	pool.close()

	return as_relations

