#!/usr/bin/env /usr/pkg/bin/python3.8

import sys

import time
import re
import numpy as np
import pandas as pd
import psycopg2

from geopy.geocoders import Nominatim


def twiddle(i):
    twid = {0 : '|', 1 : '/', 2 : '-', 4 : chr(92)} 
    sys.stdout.write(f'\r{twid[i%5]}')
    return i + 1

def haversine(coords_f, coords_i):
    """ compute great-circle distance between two points on a sphere """
    earth_radius = 6369106.0 # meters
    coords_fr = coords_f*np.pi/180
    coords_ir = coords_i*np.pi/180
    a = np.power(np.sin((coords_fr[0] - coords_ir[0])/2),2)
    b = np.cos(coords_fr[0])*np.cos(coords_ir[0])*np.power(np.sin((coords_fr[1]-coords_ir[1])/2),2)
    distance = 2*earth_radius*np.arcsin(np.sqrt(a+b))
    return distance

def load_geocache():
    """ read locations from postgres db """
    with psycopg2.connect(DB_CONNECTION) as conn:
        df = pd.read_sql_query(QUERY, conn)
        df['loc'].replace(" [0-9]{5}", "", regex = True, inplace = True)
    return df

def update_geocache(dataframe, adtfile):
    geocoder = Nominatim(user_agent = USER_AGENT)
    with open(adtfile , 'a+') as cachefile:
        cachefile.seek(0)
        geocache = cachefile.read()
        i = 0
        for each in dataframe['loc'].value_counts().items():
            location = each[0]
            if location in geocache:
                continue
            try:
                loc = geocoder.geocode(location)
                geocode = f"{location}{US}{loc.latitude}{US}{loc.longitude}{RS}"
                cachefile.write(geocode)
                time.sleep(1)
                i = twiddle(i)
            except:
                continue

def read_record(file_d):
    """ read one record from an ascii-delimted text file """
    record = ''
    char = ''
    while True:
        char = file_d.read(1)
        if char == RS: 
            return record
        if char == '':
            return None
        record = record + char

def generate_targets(all_records, target_records):
    """ select locations which are more than 50 miles from all other locations """
    with open(all_records, 'r') as geocache:
        with open(target_records, 'a+') as target:
            while True:                
                geo_rec = read_record(geocache)
                if geo_rec == None:
                    break    
                write_string = f"{geo_rec}{RS}"
                target.seek(0)
                if target.read(1) == '':
                    target.write(write_string)
                    continue
                target.seek(0)
                while True:
                    target_record = read_record(target)
                    if target_record == None:
                        print(geo_rec.split(US)[0])
                        target.write(write_string)
                        break
                    t_rec = target_record.split(US)
                    g_rec = geo_rec.split(US)
                    latlon_f = np.array([float(t_rec[1]), float(t_rec[2])])
                    latlon_i = np.array([float(g_rec[1]), float(g_rec[2])])
                    d = haversine(latlon_f, latlon_i)
                    if d < _50MILE:
                        break

def main():
    """ driver function """
    df = load_geocache()
    update_geocache(df, GEOCACHE)
    generate_targets(GEOCACHE, CITIES)
         

if __name__ == "__main__":
    _50MILE = 80467.2  # meter
    DB_CONNECTION = "dbname=infact user=pgsql"
    USER_AGENT = "jobkilla-9000"
    RS = chr(30)
    US = chr(31)
    QUERY = "SELECT loc from jobmap"
    GEOCACHE = "./geocodes.adt"
    CITIES = "./cities.adt"
    main()
