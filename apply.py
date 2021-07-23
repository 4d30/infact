#!/usr/bin/env /usr/pkg/bin/python3.8

import os
import psycopg2
import datetime

from selenium import webdriver
from selenium .webdriver.firefox.options import Options

FF_OPTS = Options()
FF_PRO = webdriver.FirefoxProfile()
FF_PRO.set_preference("general.useragent.override",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0")
driver = webdriver.Firefox(firefox_profile = FF_PRO, options=FF_OPTS)

INFACT = os.environ['INFACT']
QUERYPATH = './sql/best_jobs.sql'
DB_CONNECTION = 'dbname=infact user=pgsql'

with open(QUERYPATH,'r') as f:
   QUERY = f.read() 

conn = psycopg2.connect(DB_CONNECTION)
conn.set_client_encoding('UTF8')
cur_scroll = conn.cursor()
cur_update = conn.cursor()

def get_status():
    status = int(input('Apply status: '))
    if (1 <= status) & (status < 3):
        return status
    if status == 0:
        return None
    else:
        print("Invalid status")
        return get_status()


BASEURL = f'https://{INFACT}.com/viewjob?jk='
cur_scroll.execute(QUERY)
for each in cur_scroll:
    driver.get(BASEURL + each[0])
    status = get_status()
    if (status == 1) | (status == 2):
        update_query = f"INSERT INTO status (jk, date_, status) VALUES (%s, %s, %s)"
        today  = datetime.date.today().strftime('%Y%m%d')
        cur_update.execute(update_query, (each[0], today, status))
        conn.commit()
conn.close()

