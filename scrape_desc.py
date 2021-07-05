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

HEADERS = {
    'User-Agent': 
    ('Mozilla/5.0 (Windows NT 10.0; Win64; x64)' 
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/91.0.4472.77 Safari/537.36'),
    'Accept':
    ('text/html,application/xhtml+xml,'
    'application/xml;q=0.9,image/webp,'
    'image/apng,*/*;q=0.8'),
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.5'}

INFACT = os.environ['INFACT']
script = ''
print('Go!')

def get_listing_date(response_text):
    pattern = re.compile(r'Just posted')
    if len(pattern.findall(response_text)) == 1:
        return datetime.date.today().strftime('%Y%m%d')
    pattern = re.compile(r'.*Today')
    if len(pattern.findall(response_text)) == 1:
        return datetime.date.today().strftime('%Y%m%d')
    pattern = re.compile(r'[0-9]?[0-9]\+? days? ago')
    matches = pattern.search(response_text)
    if matches is not None:
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

with psycopg2.connect(DB_CONNECTION) as conn:
    cur = conn.cursor()
    jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
    DESC_URL = f'https://www.{INFACT}.com/viewjob?jk='
    for jk in jobmap['jk']:
        filename = './jobs/'+jk
        if os.path.isfile(filename):
            continue
        else:
            delay = SystemRandom().randrange(3,12)
            time.sleep(delay)
            URL = DESC_URL + jk
            response = requests.get(URL, headers = HEADERS )
            soup = BeautifulSoup(response.text,'html.parser')
            if 'captcha' in str(soup.find('title')).lower():
                print('Captcha!')
                cur.close()
                sys.exit()
            text = soup.get_text()
            pub_date = get_listing_date(text)
            print(jk, pub_date, delay*'#')
            if pub_date is None:
                continue
            else:
                try:
                    cur.execute("""INSERT INTO pub_dates
                            (jk, pub_date)
                            VALUES
                            (%s, %s)""",
                            (jk,get_listing_date(text)))
                except:
                    print("EXCEPTION")
                    print(jk)
                    print(get_listing_date(text))
                try:
                    text = text.split('Full Job Description')[1]
                    text = text.split('Report jobApply')[0]
                except:
                    None
                with open(filename,'w') as f:
                    f.write(text)
    cur.close()
    conn.commit()
