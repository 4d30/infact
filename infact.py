#!/usr/bin/env /usr/pkg/bin/python3.8

#coding: utf-8
import os
import sys
from random import SystemRandom
import time
import datetime
import re
import json

from bs4 import BeautifulSoup
import brotli
import requests

import pandas as pd
import psycopg2

#pd.set_option('display.max_rows', None)
#pd.set_option('display.max_columns', None)

DB_CONNECTION = "dbname=infact user=pgsql"

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

HEADERS = {
    'User-Agent': 
    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:87.0)'
    'Gecko/20100101 Firefox/87.0'),
#    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)' 
#    'AppleWebKit/537.36 (KHTML, like Gecko) '
#    'Chrome/91.0.4472.77 Safari/537.36'),
    'Accept':
    ('text/html,application/xhtml+xml,'
    'application/xml;q=0.9,image/webp,*/*;q=0.8'),
#    ('text/html,application/xhtml+xml,'
#    'application/xml;q=0.9,image/webp,'
#    'image/apng,*/*;q=0.8'),
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.5'}

INFACT = os.environ['INFACT']
script = ''
print('Go!')
with psycopg2.connect(DB_CONNECTION) as conn:
    conn.set_client_encoding('UTF8')
    cur = conn.cursor()
    jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
    with open('cities.csv', 'r') as cities:
        for city in cities:
            jobcounter = 0
            print('\n'+city.rstrip())
            for decade in range(0,200,10):
                SEARCH_URL= f'https://www.{INFACT}.com/jobs?q=data+scientist&l={city.rstrip()}&radius=50&sort=date&start={decade}'
                delay = SystemRandom().randrange(3,12)
                time.sleep(delay) #
                jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
                response = requests.get( SEARCH_URL , headers = HEADERS)
                soup = BeautifulSoup(response.text, 'html.parser')
                if 'captcha' in str(soup.find('title')).lower():
                    print('Captcha!')
                    conn.commit()
                    cur.close()
                    sys.exit()
                for js in soup.find_all('script'):
                    if 'jobmap' in str(js.contents):
                        script = js.contents[0]
                df_tmp = pd.DataFrame()
                for line in script.splitlines():
                    df_tmp = df_tmp.append(fix_crappy_json(line))
                df_tmp.reset_index(inplace=True)
                for i in range(len(df_tmp)):
                    if df_tmp.iloc[i]['jk'] not in jobmap.jk.values:
                        jobcounter = jobcounter + 1
                        sys.stdout.write(f'\r{jobcounter}')
                        sys.stdout.flush()
                        cur.execute("""INSERT INTO jobmap
                            (jk, efccid, srcid,
                            cmpid, srcname, cmp,
                            cmpesc, cmplnk, loc,
                            country, zip, city,
                            title, locid, rd,
                            qloc)
                            VALUES
                            (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                            (df_tmp.iloc[i]['jk'], df_tmp.iloc[i]['efccid'], df_tmp.iloc[i]['srcid'],
                            df_tmp.iloc[i]['cmpid'], df_tmp.iloc[i]['srcname'], df_tmp.iloc[i]['cmp'],
                            df_tmp.iloc[i]['cmpesc'], df_tmp.iloc[i]['cmplnk'], df_tmp.iloc[i]['loc'],
                            df_tmp.iloc[i]['country'], df_tmp.iloc[i]['zip'], df_tmp.iloc[i]['city'],
                            df_tmp.iloc[i]['title'], df_tmp.iloc[i]['locid'], df_tmp.iloc[i]['rd'],
                            city,))
                    else:
                        continue
    cur.close()
    conn.commit()
