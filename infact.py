#!/usr/bin/env /usr/pkg/bin/python3.8

#coding: utf-8
import os
import sys
from random import SystemRandom

import time
import datetime

import requests
import brotli
from bs4 import BeautifulSoup
import re
import json

import pandas as pd
import numpy as np
import psycopg2

pd.set_option('display.max_rows', None)
pd.set_option('display.max_columns', None)

db_connection = "dbname=infact user=pgsql"

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
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
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
	for decade in range(0,200,10):
		delay = SystemRandom().randrange(3,12)
		time.sleep(delay) # 
		sys.stdout.write(f'{decade} \r')
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
	conn.commit()

def get_listing_date(RESPONSETXT):
	p = re.compile(r'Just posted')
	if len(p.findall(RESPONSETXT)) == 1:
		return datetime.date.today().strftime('%Y%m%d')	
	p = re.compile(r'.*Today')	
	if len(p.findall(RESPONSETXT)) == 1:
		return datetime.date.today().strftime('%Y%m%d')	
	p = re.compile(r'[0-9]?[0-9]\+? days? ago')
	matches = p.search(RESPONSETXT)
	if matches != None:
		no_of_days = int(matches.group().split()[0].strip('+'))
		if no_of_days < 31:
			doc_date = datetime.date.today() - datetime.timedelta(days = no_of_days)
			return doc_date.strftime('%Y%m%d')
		else:
			print("WARNING: Publication date does not conform to coded regex")
			return None
	else:
		print("WARNING: Publication date does not conform to coded regex")
		return None
		

with psycopg2.connect(db_connection) as conn:
	cur = conn.cursor()	
	jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
	DESC_URL = f'https://www.{INFACT}.com/viewjob?jk='
	for jk in jobmap['jk']:
		filename = './jobs/'+jk
		if os.path.isfile(filename):
			continue
		else:
			delay = SystemRandom().randrange(3,12)
			print(jk, delay*'#')
			time.sleep(delay)
			URL = DESC_URL + jk
			response = requests.get(URL, headers = headers )
			soup = BeautifulSoup(response.text,'html.parser')
			text = soup.get_text()
			pub_date = get_listing_date(text)
			if pub_date == None:
				continue
			else:
				cur.execute("""INSERT INTO pub_dates
							(jk, pub_date)
							VALUES
							(%s, %s)""",
							(jk,get_listing_date(text)))
				try:
					text = text.split('Full Job Description')[1]
					text = text.split('Report jobApply')[0]
				except:
					None
				with open(filename,'w') as f:
					f.write(text)
	cur.close()
	conn.commit()
