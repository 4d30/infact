#!/usr/bin/env /usr/pkg/bin/python3.8

import os

from bs4 import BeautifulSoup
import brotli
import requests
import psycopg2

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
	cur.execute("SELECT DISTINCT cmp FROM jobmap")
	for cmp in cur:
		template = {'headquarters': None, 'revenue': None, 'employees': None, 'industry': None, 'links': None}
		URL = f"https://www.{INFACT}.com/cmp/{cmp[0].replace(' ','-')}/about"
		print(URL)
		response = requests.get(URL)
		soup = BeautifulSoup(response.text,'html.parser')
		biz_info = page_type_one(template, soup)	
		if biz_info == template:
			biz_info = page_type_two(template, soup)
		print(biz_info)	
		cur2.execute("""INSERT INTO cmpdir
			(cmp, headquarters, revenue, employees, industry, links)
			VALUES
			(%s, %s, %s, %s, %s, %s) """,
			(cmp[0], biz_info['headquarters'], biz_info['revenue'], biz_info['employees'], biz_info['industry'], biz_info['links']))
	cur2.close()
	cur.close()
