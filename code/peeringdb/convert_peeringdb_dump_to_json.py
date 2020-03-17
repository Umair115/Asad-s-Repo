#!/usr/bin/env python3

import argparse
import bz2
import glob
import json
import logging
import os.path
import sqlite3
import subprocess
import sys

def process_dump_file(dump_file):

	def dict_factory(cursor, row):
		d = {}
		for idx, col in enumerate(cursor.description):
			d[col[0]] = row[idx]
		return d

	result = {}
	
	#os.chdir(os.path.dirname(__file__))
	#os.chdir(os.path.dirname(sys.path[0]))
	#print(sys.path[0])
	#sql = subprocess.check_output(['./mysql2sqlite', dump_file]).decode('utf-8')

	# treating mysql dumps and sqlite files differently
	if dump_file.endswith('.sql'):
		sql = subprocess.check_output(['./mysql2sqlite', dump_file]).decode('utf-8')
		con = sqlite3.connect(':memory:')
		cur = con.cursor()
		cur.executescript(sql)
	else:
		con = sqlite3.connect(dump_file)
		cur = con.cursor()
	tables = cur.execute("SELECT name FROM sqlite_master WHERE type='table';").fetchall()

	con.row_factory = dict_factory
	cur = con.cursor()
	for table in tables:
		result[table[0]] = cur.execute('SELECT * FROM %s' % table).fetchall()

	return result

def process_all():
	for peeringdb_dump in glob.glob('../../data/peeringdb/sql/peeringdb*.sql*'):
		#logging.info('Processing %s', peeringdb_dump)
		json_dump = process_dump_file(peeringdb_dump)
		
		filename = os.path.basename(peeringdb_dump)
		filename = '.'.join(filename.split('.')[:-1])
		filename = '../../data/peeringdb/json_dumps/%s.json.bz2' % filename

		#logging.info('Dumping results to %s', filename)
		with bz2.open(filename, 'wt') as f:
			json.dump(json_dump, f)

if __name__ == '__main__':
	#parser = argparse.ArgumentParser(description='Convert peeringdb dumps from mysql to json')
	#parser.add_argument('--single', help='Process single file only')
	#parser.add_argument('--log', help='Set the log level.', default='INFO')
	#args = parser.parse_args()

	#logging.basicConfig(
	#	format='[%(asctime)s %(levelname)5s %(processName)s] %(message)s',
	#  	level=getattr(logging, args.log.upper(), None)
	#)

	#if args.single:
	#	logging.error('Single not implemented')
	#	sys.exit(1)
	
	process_all()
