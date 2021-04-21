#!/usr/bin/env /usr/pkg/bin/python3.8

# coding: utf-8
import os
import time
import requests
import brotli
from bs4 import BeautifulSoup
import pickle
import pandas as pd


def add_to_mapping(DICT, KEY, VALUE ):
	try:
		DICT[KEY].append(VALUE)
	except KeyError:
		DICT[KEY] = [VALUE]
	except AttributeError:
		DICT[KEY] = [DICT[KEY], VALUE]

def get_pairs(LINE):
	try:
		dict_ = LINE.split('= ')[1]
		dict_ = dict_.strip("{};")
		pairs = dict_.split(',')
		return pairs 
	except:
		None

jobmap_cache = 'jobmap.pkl'
if os.path.isfile(jobmap_cache):
	print(f'Found {jobmap_cache}')
	with open(jobmap_cache,'rb') as f:
		jobmap = pickle.load(f)
else:
	jobmap = pd.DataFrame()

headers = {
	'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/78.0.3904.108 Safari/537.36',
	'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
	'Accept-Encoding': 'gzip, deflate, br',
	'Accept-Language': 'en-US,en;q=0.5'}


SEARCH_URL= 'https://www.indeed.com/jobs?q=data+scientist&l=new+york,+ny&sort=date&start='
script = ''
print('Go!')
for decade in range(0,100,10):
	time.sleep(2)
	print(decade)
	response = requests.get( SEARCH_URL+str(decade), headers = headers)
	soup = BeautifulSoup(response.text, 'html.parser')
	for js in soup.find_all('script'):
		if 'jobmap' in str(js.contents):
			script = js.contents[0]
	tmp_map = dict()
	for line in script.splitlines():
		if 'jobmap' in line:
			pairs = get_pairs(line)
			if len(pairs) > 1:
				for pair in pairs:
					if ':' in pair:
						kv_pair = pair.split(':')
						add_to_mapping(tmp_map,
								kv_pair[0],
								kv_pair[1].strip("'"))
	df_tmp = pd.DataFrame(tmp_map)
	if not os.path.isfile(jobmap_cache):
		jobmap = pd.DataFrame(columns=df_tmp.columns)
	for i in range(0,len(df_tmp)):
		if df_tmp.iloc[i,0] not in jobmap.jk.values:
			print(True)
			jobmap = jobmap.append(df_tmp.iloc[i,:])
		else:
			continue
DESC_URL = 'https://www.indeed.com/viewjob?jk='
for jk in jobmap['jk']:
	filename = './jobs/'+jk
	if os.path.isfile(filename):
		continue
	else:
		print(jk)
		URL = DESC_URL + jk
		response = requests.get(URL, headers = headers )
		soup = BeautifulSoup(response.text,'html.parser')
		text = soup.get_text()
		try:
			text = text.split('Full Job Description')[1]
			text = text.split('Report jobApply')[0]
		except:
			None
		with open(filename,'w') as f:
			f.write(text)

jobmap['mtime'] = list(map(lambda x: os.path.getmtime(f'./jobs/{x}'),jobmap['jk']))
with open(jobmap_cache,'wb') as f:
	pickle.dump(jobmap,f)
