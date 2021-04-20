#!/usr/bin/env /usr/pkg/bin/python3.8

import os
import readline
import requests
from bs4 import BeautifulSoup

ROLE = 'Data Scientist'
LOCATION = 'New York, NY'
URL = f'https://www.indeed.com/jobs?q={ROLE}&l={LOCATION}&sort=date'

print(URL)

filename = './cache.txt'

if os.path.isfile(filename):
    print('Cache found!')
    with open(filename,'r') as f:
        html = f.read()
else:
    print('Requesting URL...')
    response = requests.get(URL)
    html = response.text
    with open(filename,'w') as f:
        f.write(html)

soup = BeautifulSoup(html,'html.parser')

for js in soup.find_all('script'):
    if 'jobmap' in str(js.contents):
        print(js)
