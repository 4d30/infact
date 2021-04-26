#!/usr/bin/env /usr/pkg/bin/python3.8

# This script takes a jobkey and specified number of 
# grams and then calculates the tf-df for all constituent 
# terms

import os
import sys
import multiprocessing as mp
import time
import pickle

import pandas as pd
import numpy as np

from nltk import pos_tag
from nltk.tokenize import word_tokenize, sent_tokenize, RegexpTokenizer
from nltk.probability import FreqDist, Counter
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.util import ngrams

pd.set_option('display.max_rows', None)

# Corpus location
filepath = './jobs/'

jobmap_cache = './jobmap.pkl'
with open(jobmap_cache,'rb') as f:
	jobmap = pickle.load(f)

stop_words = set(stopwords.words("english"))
wnl = WordNetLemmatizer()
tokenizer = RegexpTokenizer(r'\w+')
n_docs = len(jobmap)

df_corpus = pd.read_csv('./grams.csv')
df_corpus.set_index('term', inplace=True)

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

QUERY = sys.argv[1]
NGRAMS = int(sys.argv[2])


term_counter = Counter()
with open(filepath+QUERY, 'rb') as f:
	sentences = get_sentences(f)
	lemmas = map(get_lemmas, sentences)
	term_counter.update(get_terms(lemmas))

df = pd.DataFrame.from_dict(term_counter, orient='index', dtype=np.uint64, columns=['term_count'])
df.index = list(map(lambda x: ' '.join(x),df.index))
df['doc_count'] = df_corpus.loc[df.index]['doc_count']
df['tf'] = 1 + np.log(df['term_count'])#df['term_count']/sum(df['term_count'])
df['idf'] = np.log(n_docs/df['doc_count'])
df['tf-idf'] = df['tf']*df['idf']
#print(list(map(lambda x: len(x.split(' ')) == 2, df.index)))

print(df[list(map(lambda x: len(x.split(' ')) == NGRAMS, df.index))].sort_values(by='tf-idf', ascending= False))
