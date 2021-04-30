#!/usr/bin/env /usr/pkg/bin/python3.8

# coding: utf-8
import os
import time
import requests
import brotli
from bs4 import BeautifulSoup
import re
import json
import pickle
import pandas as pd
import psycopg2

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

infact = os.environ['INFACT']

#pandas.read_sql_query("SELECT * FROM jobmap",conn)

#with psycopg2.connect("dbname=infact user=pgsql") as conn:
#	print(type(pd.read_sql_query("SELECT jk FROM jobmap",conn)))

#	with conn.cursor() as cur:
#		cur.execute("SELECT * FROM jobmap")
#		print(cur.fetchall())


#def add_to_mapping(DICT, KEY, VALUE ):
#	try:
#		DICT[KEY].append(VALUE)
#	except KeyError:
#		DICT[KEY] = [VALUE]
#	except AttributeError:
#		DICT[KEY] = [DICT[KEY], VALUE]
#
#def get_pairs(LINE):
#	try:
#		dict_ = LINE.split('= ')[1]
#		dict_ = dict_.strip("{};")
#		pairs = dict_.split(',')
#		return pairs 
#	except:
#		None
#
#jobmap_cache = 'jobmap.pkl'
#if os.path.isfile(jobmap_cache):
#	print(f'Found {jobmap_cache}')
#	with open(jobmap_cache,'rb') as f:
#		jobmap = pickle.load(f)
#
#headers = {
#	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
#	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
#	'Accept-Encoding': 'gzip, deflate, br',
#	'Accept-Language': 'en-US,en;q=0.5'}
#
#SEARCH_URL= f'https://www.{INFACT}.com/jobs?q=data+scientist&l=new+york,+ny&sort=date&start='
#script = ''
#print('Go!')
#
#def fix_crappy_json(LINE):
#	line = LINE
#	tmp = pd.DataFrame()
#	if line.startswith('jobmap'):
#		line = re.sub('^.*= ','',line)
#		line = re.sub(r',(\w+):',',"\\1":',line)
#		line = re.sub(r'{(\w+):','{"\\1":',line)
#		line = re.sub(r"'",'"',line)
#		line = re.sub(';$','',line)
#		try:
#			return tmp.append(json.loads(line),ignore_index=True)
#		except:
#			print(LINE)
#			print(line)
#	else:
#		return None
#for decade in range(0,100,10):
#	time.sleep(2)
#	print(decade)
#	response = requests.get( SEARCH_URL+str(decade), headers = headers)
#	soup = BeautifulSoup(response.text, 'html.parser')
#	for js in soup.find_all('script'):
#		if 'jobmap' in str(js.contents):
#			script = js.contents[0]
#	df_tmp = pd.DataFrame()
#	for line in script.splitlines():
#		df_tmp = df_tmp.append(fix_crappy_json(line))
#	df_tmp.set_index('num',inplace=True)
#	if not os.path.isfile(jobmap_cache):
#		jobmap = pd.DataFrame(columns=df_tmp.columns)
#	for index, jk in enumerate(df_tmp.jk.values):
#		if jk not in jobmap.jk.values:
#			print(True, jk)
#			jobmap = jobmap.append(df_tmp.iloc[index])
#		else:
#			continue
#
#DESC_URL = f'https://www.{INFACT}.com/viewjob?jk='
#for jk in jobmap['jk']:
#	filename = './jobs/'+jk
#	if os.path.isfile(filename):
#		continue
#	else:
#		print(jk)
#		URL = DESC_URL + jk
#		response = requests.get(URL, headers = headers )
#		soup = BeautifulSoup(response.text,'html.parser')
#		text = soup.get_text()
#		try:
#			text = text.split('Full Job Description')[1]
#			text = text.split('Report jobApply')[0]
#		except:
#			None
#		with open(filename,'w') as f:
#			f.write(text)
#
#jobmap['mtime'] = list(map(lambda x: os.path.getmtime(f'./jobs/{x}'),jobmap['jk']))
#with open(jobmap_cache,'wb') as f:
#	pickle.dump(jobmap,f)
