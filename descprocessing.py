#!/usr/bin/env /usr/pkg/bin/python3.8

# This script calculates tf & idf for all of the terms in all of the 
# documents in the directory specified. The process occurs over two
# loops. The first discovers and count all of the terms. The second
# counts the number of documents in which each term appears. 

import os
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


# Corpus location
filepath = './jobs/'

# Open Cache
jobmap_cache = './jobmap.pkl'
with open(jobmap_cache,'rb') as f:
	jobmap = pickle.load(f)


stop_words = set(stopwords.words("english"))
wnl = WordNetLemmatizer()
tokenizer = RegexpTokenizer(r'\w+')
n_docs = len(jobmap)

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

def count_terms_in_slice(PATH, PARTIAL_LIST_OF_JK, TERMCOUNTER):
	for index, jk in enumerate(PARTIAL_LIST_OF_JK):
		with open(PATH + jk, 'rb') as f:
			sentences = get_sentences(f)
			lemmas = map(get_lemmas, sentences)
			TERMCOUNTER.update(get_terms(lemmas))
	return TERMCOUNTER

def divide_work(FULL_LIST_OF_JK):
	n_cpus = mp.cpu_count()
	job_length = np.floor(len(FULL_LIST_OF_JK)/n_cpus)
	work_schedule = np.zeros((n_cpus,2), dtype = np.uint64)
	for i in range(0, n_cpus):
		distance = i*job_length
		work_schedule[i,0] = distance
		if i != (n_cpus - 1):
			work_schedule[i,1] = job_length
		else:
			work_schedule[i,1] = len(FULL_LIST_OF_JK) - distance
	return work_schedule

def count_terms_process_target(PATH, PARTIAL_LIST_OF_JK, TERMCOUNTER, PIPE):	
	counter_obj = count_terms_in_slice(PATH, PARTIAL_LIST_OF_JK, TERMCOUNTER)
	PIPE.send(counter_obj)
	PIPE.close()

pipes = []
processes = []
term_counter = Counter()
start_time = time.time()
# Identify and count terms
if __name__ == '__main__':
	for job in divide_work(jobmap['jk']):
		workslice = jobmap['jk'][job[0]:job[0]+job[1]]
		pipe_recv, pipe_send = mp.Pipe(False)
		pipes.append(pipe_recv)
		p = mp.Process(target=count_terms_process_target, 
						args=('./jobs/', 
								workslice, 
								Counter(), 
								pipe_send))
		processes.append(p)
		p.start()
	
	for i in range(len(processes)):
		term_counter.update(pipes[i].recv())
		pipes[i].close()
		processes[i].join()
print(f'Term Loop:\tTime elapsed: {time.time() - start_time:.2f} sec\n')	

def are_terms_in_doc(TERMS, FILENAME):
	termsindoc = FreqDist()
	with open (FILENAME, 'rb') as f:
		sent = get_sentences(f)
		lemmas = map(get_lemmas, sent)
		termsindoc.update(get_terms(lemmas))
	return list(map(lambda x: x in termsindoc.keys(), TERMS))

def count_doc_freq_in_slice(PATH, PARTIAL_LIST_OF_JK, TERMS, ARRAY):
	pcount_array = ARRAY	
	for jk in PARTIAL_LIST_OF_JK:
		filename = PATH + jk
		pcount_array = pcount_array + are_terms_in_doc(TERMS, filename)
	return pcount_array 

def count_doc_process_target(PATH, PARTIAL_LIST_OF_JK, TERMS, ARRAY, PIPE):
	count_array = count_doc_freq_in_slice(PATH, PARTIAL_LIST_OF_JK, TERMS, ARRAY)
	PIPE.send(count_array)
	PIPE.close()
	
pipes = []
processes = []
doc_counter = np.zeros((len(term_counter.keys())), dtype = np.uint64)
start_time = time.time()
# Count number of docs which contain each term found earlier
if __name__ == '__main__':
	for job in divide_work(jobmap['jk']):
		workslice = jobmap['jk'][job[0]:job[0]+job[1]]
		pipe_recv, pipe_send = mp.Pipe(False)
		pipes.append(pipe_recv)
		p = mp.Process(target=count_doc_process_target,
						args=('./jobs/', 
							workslice, 
							list(term_counter.keys()), 
							np.zeros((len(term_counter.keys())), dtype = np.longlong),
							pipe_send))
		processes.append(p)
		p.start()
	
	for i in range(len(processes)):
		doc_counter = doc_counter + pipes[i].recv()
		pipes[i].close()
		processes[i].join()

print(f'Doc Loop:\tTime elapsed: {time.time() - start_time:.2f} sec\n')	
df = pd.DataFrame.from_dict(term_counter, orient='index', dtype = int, columns = ['term_count'])
df['doc_count'] = doc_counter
df['tf_corpus'] = df['term_count']/sum(df['term_count'])
df['idf'] = np.log(n_docs/df['doc_count'])
df['tf-idf_corpus'] = df['tf_corpus']*df['idf']

df.index = list(map(lambda x: ' '.join(x), df.index))
df.index.name = 'term'

print(df.sort_values(by='tf-idf_corpus', ascending=False)[0:50])
df.sort_values(by='tf-idf_corpus', ascending=False).to_csv('./grams.csv')
