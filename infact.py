#!/usr/bin/env /usr/pkg/bin/python3.8

#coding: utf-8
import os
import sys
from random import SystemRandom
import subprocess
import time
import re
import json

from bs4 import BeautifulSoup

import pandas as pd
import psycopg2

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

def fix_crappy_json(line_):
    """ reformats html-garbage into uzbl json"""
    line = line_
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
            print(line_)
            print(line)
            return None
    else:
        return None

def insert_into_jobmap(cursor, dataframe, i):
    """ seperated from main() for cleanliness """
    cursor.execute("""INSERT INTO jobmap
        (jk, efccid, srcid,
        cmpid, srcname, cmp,
        cmpesc, cmplnk, loc,
        country, zip, city,
        title, locid, rd)
        VALUES
        (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (dataframe.iloc[i]['jk'], dataframe.iloc[i]['efccid'], dataframe.iloc[i]['srcid'],
        dataframe.iloc[i]['cmpid'], dataframe.iloc[i]['srcname'], dataframe.iloc[i]['cmp'],
        dataframe.iloc[i]['cmpesc'], dataframe.iloc[i]['cmplnk'], dataframe.iloc[i]['loc'],
        dataframe.iloc[i]['country'], dataframe.iloc[i]['zip'], dataframe.iloc[i]['city'],
        dataframe.iloc[i]['title'], dataframe.iloc[i]['locid'], dataframe.iloc[i]['rd'],))

def init():
    """ initializes a dict of local variables"""
    init_dict = {}
    db_connection = 'dbname=infact user=pgsql'
    ff_opts = Options()
    ff_pro = webdriver.FirefoxProfile()
    ff_pro.set_preference("general.useragent.override",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0")
    #firefox_opts.headless = True
    init_dict['driver'] = webdriver.Firefox(firefox_profile = ff_pro, options=ff_opts)
    init_dict['INFACT'] = os.environ['INFACT']
    init_dict['script'] = ''
    init_dict['conn'] = psycopg2.connect(db_connection)
    init_dict['conn'].set_client_encoding('UTF8')
    init_dict['cur'] = init_dict['conn'].cursor()
    return init_dict 

def gen_url(init_dict, city, decade):
    """ creates a URL string to which selenium will navigate """
    base_url = f'https://www.{init_dict["INFACT"]}.com/'
    return base_url + f'jobs?q=data+scientist&l={city.rstrip()}&radius=50&sort=date&start={decade}'

def read_record(file_d):
    record = ''
    char = ''
    while True:
        char = file_d.read(1)
        if char == RS:
            return record
        if char == '':
            return None
        record = record + char

def sound_alarm():
    print('Captcha!')
    subprocess.call(['xterm', '-e', './alarm.sh'])
    input("Press enter to continue...")

def main():
    """ drives selenium and updates SQL table """
    init_dict = init()
    jobmap = pd.read_sql_query("SELECT jk FROM jobmap",init_dict['conn'])
    cities = open('./cities.adt', 'r')
    while True:
        record = read_record(cities)
        if record == None:
            break
        city = record.split(US)[0]
        jobcounter = 0
        print('\n'+city.rstrip())
        old_jobs = [False]
        for decade in range(0,200,10):
            if all(old_jobs):
                break
            old_jobs = []
            search_url = gen_url(init_dict, city, decade)
            delay = SystemRandom().randrange(3,12)
            time.sleep(delay)
            jobmap = pd.read_sql_query("SELECT jk FROM jobmap",init_dict['conn'])
            try:
                init_dict['driver'].get( search_url )
            except selenium.common.exceptions.WebDriverException:
                sound_alarm()
            response = init_dict['driver'].page_source
            soup = BeautifulSoup(response, 'html.parser')
            if 'captcha' in str(soup.find('title')).lower():
                sound_alarm()
            response = init_dict['driver'].page_source
            soup = BeautifulSoup(response, 'html.parser')
            for javascript in soup.find_all('script'):
                if 'jobmap' in str(javascript.contents):
                    init_dict['script'] = javascript.contents[0]
            df_tmp = pd.DataFrame()
            for line in init_dict['script'].splitlines():
                df_tmp = df_tmp.append(fix_crappy_json(line))
            df_tmp.reset_index(inplace=True)
            for i in range(len(df_tmp)):
                if df_tmp.iloc[i]['jk'] not in jobmap.jk.values:
                    old_jobs.append(False)
                    jobcounter = jobcounter + 1
                    sys.stdout.write(f'\r{jobcounter}')
                    sys.stdout.flush()
                    insert_into_jobmap(init_dict['cur'], df_tmp, i)
                    init_dict['conn'].commit()
                else:
                    old_jobs.append(True)
                    continue
    cities.close()
    init_dict['cur'].close()
    init_dict['conn'].commit()
    init_dict['conn'].close()
    init_dict['driver'].quit()

if __name__ == '__main__':
    RS = chr(30)
    US = chr(31)
    main()
