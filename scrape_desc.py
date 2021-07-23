#!/usr/bin/env /usr/pkg/bin/python3.8

#coding: utf-8
import os
import sys
import subprocess
from random import SystemRandom
import time
import datetime
import re

from bs4 import BeautifulSoup

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

import pandas as pd
import psycopg2

FF_OPTS = Options()
FF_PRO = webdriver.FirefoxProfile()
FF_PRO.set_preference("general.useragent.override",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0")
#firefox_opts.headless = True
driver= webdriver.Firefox(firefox_profile = FF_PRO, options=FF_OPTS)

DB_CONNECTION = "dbname=infact user=pgsql"
INFACT = os.environ['INFACT']
script = ''
print('Go!')

def get_listing_date(soup):
    footer_txt = str(soup.find_all(class_ = 'jobsearch-JobMetadataFooter')[0])
    pattern = re.compile(r'Just posted')
    if len(pattern.findall(footer_txt)) == 1:
        return datetime.date.today().strftime('%Y%m%d')
    pattern = re.compile(r'Today')
    if len(pattern.findall(footer_txt)) == 1:
        return datetime.date.today().strftime('%Y%m%d')
    pattern = re.compile(r'[0-9]?[0-9]\+? days? ago')
    matches = pattern.search(str(footer_txt))
    if matches is not None:
        no_of_days = int(matches.group().split()[0].strip('+'))
        if no_of_days < 30:
            doc_date = datetime.date.today() - datetime.timedelta(days = no_of_days)
            return doc_date.strftime('%Y%m%d')
        else:
            return '19000101'
    else:
        return None

conn = psycopg2.connect(DB_CONNECTION)
cur = conn.cursor()
jobmap = pd.read_sql_query("SELECT jk FROM jobmap",conn)
DESC_URL = f'https://www.{INFACT}.com/viewjob?jk='
for jk in jobmap['jk']:
    filename = './jobs/'+jk
    if os.path.isfile(filename):
        continue
    else:
        delay = SystemRandom().randrange(5,12)
        time.sleep(delay)
        URL = DESC_URL + jk
        try:
            driver.get(URL)
        except:
            continue
        response = driver.page_source
        soup = BeautifulSoup(response,'html.parser')
        if 'captcha' in str(soup.find('title')).lower():
            print('Captcha!')
            subprocess.call(['xterm', '-e', './alarm.sh'])
            input("Press enter to continue...")
        response = driver.page_source
        soup = BeautifulSoup(response,'html.parser')
        try:
            pub_date = get_listing_date(soup)
        except:
            print(jk)
            continue
        print(jk, pub_date, delay*'#')
        if pub_date is None:
            continue
        else:
            try:
                cur.execute("""INSERT INTO pub_dates
                        (jk, pub_date)
                        VALUES
                        (%s, %s)""",
                        (jk,get_listing_date(soup)))
                conn.commit()
            except:
                print("EXCEPTION")
                print(jk)
                print(get_listing_date(soup))
            with open(filename,'w') as f:
                try:
                   f.write(driver.find_element_by_id("jobDescriptionText").text)
                except:
                   print("could not write to file") 
cur.close()
conn.close()
