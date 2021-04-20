#!/usr/bin/env /usr/pkg/bin/python3.8
import os
import pandas as pd
import pickle

import matplotlib as mpl
import matplotlib.pyplot as plt

mpl.use('TkAgg')

with open('./jobmap.pkl','rb') as f:
	df_jobmap = pickle.load(f)


with open('./grams.csv','r') as f:
	df_grams = pd.read_csv(f)
	df_grams.set_index('term',inplace=True)


print(df_grams[:50])

fig = plt.figure(figsize=(8,6))
df_grams['tf-idf'][0:50].plot(kind='bar')
plt.ylabel('tf-idf')
plt.tight_layout()
plt.show()
#for i in range(0,len(df.cmp.value_counts())):
#	print(df.cmp.value_counts().index[i],df.cmp.value_counts()[i])
#
#fig = plt.figure(figsize=(6,6))
#df.cmp.value_counts()[:20].plot(kind='bar')
#plt.title('Job Listings for Data Scientist in NYC')
#plt.xlabel('Company')
#plt.ylabel('Number of Postings')
#plt.tight_layout()
#plt.show()
