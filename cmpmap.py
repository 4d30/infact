#!/usr/bin/env /usr/pkg/bin/python3.8

import os

from random import SystemRandom
import time

from bs4 import BeautifulSoup
import psycopg2

from selenium import webdriver
from selenium.webdriver.firefox.options import Options

FF_OPTS = Options()
FF_PRO = webdriver.FirefoxProfile()
FF_PRO.set_preference("general.useragent.override",
"Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0")
#firefox_opts.headless = True
driver= webdriver.Firefox(firefox_profile = FF_PRO, options=FF_OPTS)


INFACT = os.environ['INFACT']

headers = { 
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.114 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'en-US,en;q=0.5'}

def page_type_one(TEMPLATE, SOUP):
    for category in soup.find_all("div", class_="cmp-CompanyDetailSection"):
        TEMPLATE[str(category.h3.text).lower()] = category.div.text
    return TEMPLATE


def page_type_two(TEMPLATE, SOUP):
    for category in soup.find_all("div", class_="cmp-AboutBasicCompanyDetailsWidget"):
        for detail in category.contents:
            if 'data-testid' in detail.attrs:
                TEMPLATE[str(detail['data-testid']).lower()] = detail.text
    return TEMPLATE 

dbcn = 'dbname=infact user=pgsql'
with psycopg2.connect(dbcn) as conn:
    conn.set_client_encoding('UTF8')
    cur = conn.cursor()
    cur2 = conn.cursor()
    cur3 = conn.cursor()
    cur.execute("SELECT DISTINCT cmp FROM jobmap")
    for cmp in cur:
        fuse = False
        cur3.execute("SELECT DISTINCT cmp FROM cmpdir")
        for cmp3 in cur3:
            if cmp == cmp3 and cmp3 is not None: 
                fuse = True
                break
        if fuse == True:
            continue    
        else:
            delay = SystemRandom().randrange(3,12)
            print(cmp[0], delay*'#')
            time.sleep(delay)
            template = {'headquarters': None, 'revenue': None, 'employees': None, 'industry': None, 'links': None}
            URL = f"https://www.{INFACT}.com/cmp/{cmp[0].replace(' ','-')}/about"
            print(URL)
            driver.get(URL)
            response = driver.page_source
            soup = BeautifulSoup(response, 'html.parser')
            if 'captcha' in str(soup.find('title')).lower():                
                print('Captcha!')
                conn.commit()
                cur.close()
                sys.exit()
            response = driver.page_source
            soup = BeautifulSoup(response, 'html.parser')
            biz_info = page_type_one(template, soup)    
            if biz_info == template:
                biz_info = page_type_two(template, soup)
            print(biz_info)    
            cur2.execute("""INSERT INTO cmpdir
                (cmp, headquarters, revenue, employees, industry, links)
                VALUES
                (%s, %s, %s, %s, %s, %s) """,
            (cmp[0], biz_info['headquarters'], biz_info['revenue'], biz_info['employees'], biz_info['industry'], biz_info['links']))
            conn.commit()
    cur2.close()
    cur.close()
