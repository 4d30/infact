#!/usr/bin/env /usr/pkg/bin/python3.8

# This script calculates tf-idf for all of the terms in all of the 
# documents in the directory specified. The process occurs over two
# loops. The first discovers and count all of the terms. The second
# counts the number of documents in which each term appears. 

import os
import multiprocessing as mp
import pickle

import pandas as pd
import numpy as np

from nltk import pos_tag
from nltk.tokenize import word_tokenize, sent_tokenize, RegexpTokenizer
from nltk.probability import FreqDist
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.util import ngrams


def tag_nltk2wordnet(nltk_tag):
	if nltk_tag.startswith('J'):
		return wordnet.ADJ
	elif nltk_tag.startswith('V'):
		return wordnet.VERB
	elif nltk_tag.startswith('N'):
		return wordnet.NOUN
	elif nltk_tag.startswith('R'):
		return wordnet.ADV
	else:		  
		return wordnet.NOUN

jobmap_cache = './jobmap.pkl'
with open(jobmap_cache,'rb') as f:
	jobmap = pickle.load(f)

stop_words = set(stopwords.words("english"))
wnl = WordNetLemmatizer()
tokenizer = RegexpTokenizer(r'\w+')

# Corpus location
filepath = './jobs/'

MAX_GRAMS = 4
term_counts = []
df= []

for i in range(0,MAX_GRAMS):
	df.append(pd.DataFrame())
	term_counts.append(FreqDist())

n_docs = len(os.listdir(filepath))

LIMIT=1e9

# Count number of occurances for each term in all files
#for i, filename in enumerate(os.listdir(filepath)):
#	if i >= LIMIT: # Throttle for debugging, remove 
#		break
#	else:
#		with open(filepath+filename, 'rb') as f:
#			raw = f.read().decode('utf8')
#			raw = raw.lower()
#			sent = sent_tokenize(raw)
#			for s in sent:
#				words = tokenizer.tokenize(s)
#				tagged = pos_tag(words)
#				filtered_words = [t for t in tagged if not t[0] in stop_words]
#				lemmatized_words = [wnl.lemmatize(w[0],pos=tag_nltk2wordnet(w[1])) for w in filtered_words]
#				for k in range(0,MAX_GRAMS): 
#					grams = ngrams(lemmatized_words, k + 1)
#					term_counts[k].update(grams)
#
#for i in range(0,MAX_GRAMS):
#	df[i] = pd.DataFrame.from_dict(term_counts[i],orient='index', dtype = int, columns=['term_count'])
#	df[i]['doc_count'] = np.zeros(len(df[i]),dtype = int)
#	df[i].index.name = 'term'

# Count the number of documents in which each term from above appears
def get_sentences(FILE):
	raw = FILE.read().decode('utf8')
	raw = raw.lower()
	sent = sent_tokenize(raw)
	return sent


def get_lemmas(SENT):
	words = tokenizer.tokenize(SENT)
	tagged = pos_tag(words)
	filtered_words = [t for t in tagged if not t[0] in stop_words]
	lemmas = [wnl.lemmatize(w[0], pos=tag_nltk2wordnet(w[1])) for w in filtered_words]
	return lemmas

def get_terms(LEMMAS):
	terms = FreqDist()
	for sent in LEMMAS:
		for num in range(1,4):
			grams = ngrams(sent, num)
			terms.update(grams)
	return terms

for index, jk in enumerate(jobmap['jk']):
	if index > 0:
		break
	with open (filepath+jk, 'rb') as f:
		sentences = get_sentences(f)
		lemmas = map(get_lemmas, sentences)
		terms = get_terms(lemmas)
		print(terms.most_common(10))


#for i, filename in enumerate(os.listdir(filepath)):
#	if i >= LIMIT:
#		break
#	else:
#		with open(filepath + filename, 'rb') as f:
#			gramdist = []
#			for j in range(0,MAX_GRAMS):
#				gramdist.append(FreqDist())
#			raw = f.read().decode('utf8')
#			raw = raw.lower()
#			sent = sent_tokenize(raw)
#			for s in sent:
#				words = tokenizer.tokenize(s)
#				tagged = pos_tag(words)
#				filtered_words = [t for t in tagged if not t[0] in stop_words]
#				lemmatized_words = [wnl.lemmatize(w[0],pos=tag_nltk2wordnet(w[1])) for w in filtered_words]
#				for k in range(0,MAX_GRAMS):
#					gramdist[k].update(ngrams(lemmatized_words, k + 1))
#			for j in range(0,MAX_GRAMS):
#				df[j]['doc_count'] = df[j]['doc_count'] + list(map(lambda x: x in gramdist[j],list(df[j].index)))

#for i in range(0,MAX_GRAMS):
#	df[i]['tf'] = df[i]['term_count']/sum(df[i]['term_count']) # Calculate tf
#	df[i]['idf'] = np.log(n_docs/df[i]['doc_count'])	#Calculate idf
#	df[i]['tf-idf'] = df[i]['tf']*df[i]['idf']			#Calculate tf-idf
#
#df_concat = pd.DataFrame()
#for i in range(0,MAX_GRAMS):
#	df_concat = df_concat.append(df[i])	
#df_concat.index = list(map(lambda x: ' '.join(x),df_concat.index)) # Prettify terms
#df_concat.index.name = 'term'
#
#print(df_concat.sort_values(by='tf-idf', ascending = False)[0:50])
#df_concat.sort_values(by='tf-idf', ascending = False).to_csv('./grams.csv')
