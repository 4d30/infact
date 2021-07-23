#!/usr/bin/env /usr/pkg/bin/python3.8

#coding: utf-8
import os
import sys
from random import SystemRandom
import subprocess
import time
import datetime
import re
import json

from bs4 import BeautifulSoup

import pandas as pd
import psycopg2


from selenium import webdriver
from selenium.webdriver.firefox.options import Options

FF_OPTS = Options()
FF_PRO = webdriver.FirefoxProfile()
FF_PRO.set_preference("general.useragent.override",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0")
#firefox_opts.headless = True
driver= webdriver.Firefox(firefox_profile = FF_PRO, options=FF_OPTS)

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

INFACT = os.environ['INFACT']
script = ''
print('Go!')
conn = psycopg2.connect(DB_CONNECTION)
conn.set_client_encoding('UTF8')
cur = conn.cursor()
jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
cities = open('cities.csv', 'r')
for city in cities:
    jobcounter = 0
    print('\n'+city.rstrip())
    old_jobs = [False]
    for decade in range(0,200,10):
        if all(old_jobs):
            continue
        else:
            old_jobs = []
            SEARCH_URL= f'https://www.{INFACT}.com/jobs?q=data+scientist&l={city.rstrip()}&radius=50&sort=date&start={decade}'
            delay = SystemRandom().randrange(3,12)
            time.sleep(delay) 
            jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
            driver.get( SEARCH_URL )
            response = driver.page_source
            soup = BeautifulSoup(response, 'html.parser')
            if 'captcha' in str(soup.find('title')).lower():
                print('Captcha!')
                subprocess.call(['xterm', '-e', './alarm.sh'])
                input("Press enter to continue...")
            response = driver.page_source
            soup = BeautifulSoup(response, 'html.parser')
            for js in soup.find_all('script'):
                if 'jobmap' in str(js.contents):
                    script = js.contents[0]
            df_tmp = pd.DataFrame()
            for line in script.splitlines():
                df_tmp = df_tmp.append(fix_crappy_json(line))
            df_tmp.reset_index(inplace=True)
            for i in range(len(df_tmp)):
                if df_tmp.iloc[i]['jk'] not in jobmap.jk.values:
                    old_jobs.append(False)
                    jobcounter = jobcounter + 1
                    sys.stdout.write(f'\r{jobcounter}')
                    sys.stdout.flush()
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
                    conn.commit()
                else:
                    old_jobs.append(True)
                    continue
cities.close()
cur.close()
conn.commit()
conn.close()
driver.quit()
