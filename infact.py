#!/usr/bin/env /usr/pkg/bin/python3.8

#coding: ascii
import os
import time
import requests
import brotli
from bs4 import BeautifulSoup
import re
import json
import pickle
import pandas as pd
import numpy as np
import psycopg2

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

db_connection = "dbname=infact user=pgsql"

#jobmap_cache = 'jobmap.pkl'
#if os.path.isfile(jobmap_cache):
#	print(f'Found {jobmap_cache}')
#	with open(jobmap_cache,'rb') as f:
#		jobmap = pickle.load(f)
def fix_crappy_json(LINE):
	line = LINE
	tmp = pd.DataFrame()
	if line.startswith('jobmap'):
		line = re.sub('^.*= ','',line)
		line = re.sub(r',(\w+):',',"\\1":',line)
		line = re.sub(r'{(\w+):','{"\\1":',line)
		line = re.sub(r"'",'"',line)
		line = re.sub(';$','',line)
		try:
			return tmp.append(json.loads(line),ignore_index=True)
		except:
			print("CRAPPY JSON EXCEPTION")
			print(LINE)
			print(line)
			return None
	else:
		return None

headers = {
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
	'Accept-Encoding': 'gzip, deflate, br',
	'Accept-Language': 'en-US,en;q=0.5'}

INFACT = os.environ['INFACT']
SEARCH_URL= f'https://www.{INFACT}.com/jobs?q=data+scientist&l=new+york,+ny&sort=date&start='
script = ''
print('Go!')
with psycopg2.connect(db_connection) as conn:
	conn.set_client_encoding('UTF8')
	cur = conn.cursor()
	jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
	for decade in range(0,100,10):
		time.sleep(2)
		print(decade)
		jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
		response = requests.get( SEARCH_URL + str(decade), headers = headers)
		soup = BeautifulSoup(response.text, 'html.parser')
		for js in soup.find_all('script'):
			if 'jobmap' in str(js.contents):
				script = js.contents[0]
		df_tmp = pd.DataFrame()
		for line in script.splitlines():
			df_tmp = df_tmp.append(fix_crappy_json(line))
		df_tmp.reset_index(inplace=True)
		for i in range(len(df_tmp)):
			if df_tmp.iloc[i]['jk'] not in jobmap.jk.values:
				print(df_tmp.iloc[i]['jk'])
				cur.execute("""INSERT INTO jobmap
					(jk, efccid, srcid,
					cmpid, srcname, cmp,
					cmpesc, cmplnk, loc,
					country, zip, city,
					title, locid, rd) 
					VALUES 
					(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
					(df_tmp.iloc[i]['jk'], df_tmp.iloc[i]['efccid'], df_tmp.iloc[i]['srcid'],
					df_tmp.iloc[i]['cmpid'], df_tmp.iloc[i]['srcname'], df_tmp.iloc[i]['cmp'], 
					df_tmp.iloc[i]['cmpesc'], df_tmp.iloc[i]['cmplnk'], df_tmp.iloc[i]['loc'], 
					df_tmp.iloc[i]['country'], df_tmp.iloc[i]['zip'], df_tmp.iloc[i]['city'], 
					df_tmp.iloc[i]['title'], df_tmp.iloc[i]['locid'], df_tmp.iloc[i]['rd'],))
			else:
				continue
	cur.close()
	
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
