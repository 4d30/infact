#!/usr/bin/env /usr/pkg/bin/python3.8

import os
import sys
import multiprocessing as mp

import time
import datetime

import io
import csv
import psycopg2

import numpy as np

from nltk import pos_tag
from nltk.tokenize import word_tokenize, sent_tokenize, RegexpTokenizer
from nltk.probability import FreqDist, Counter
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.util import ngrams

DB_CONNECTION = "dbname=infact user=pgsql"
RESUME_FILE = "./sample.txt"
CORPUS_PATH = "./jobs/"
JK = "0035d750467e0b52"


stop_words = list(stopwords.words("english"))
tokenizer = RegexpTokenizer(r'\w+')
wnl = WordNetLemmatizer()

def get_freqdist(filename):
    
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
    
    def get_sentences(file):
        with open(file, 'r') as f:
            raw = f.read()
        raw = raw.lower()
        sent = sent_tokenize(raw)
        return sent
    
    def tok_and_tag(SENT):
        words = [tokenizer.tokenize(s) for s in SENT]
        tagged = [pos_tag(w) for w in words]
        return tagged
    
    def filter_words(tagged):
        filtered_words = []
        for t in tagged:
            if len(t) == 0:
                continue
            else:
                if t[0] not in stop_words:
                    filtered_words.append(t)
        return filtered_words 
    
    def get_lemmas(tagged):
        lemmas = [] 
        for each in tagged:
            lemmas.append([wnl.lemmatize(w[0], pos=tag_nltk2wordnet(w[1])) for w in each])
        return lemmas
    
    def get_grams(LEMMAS):
        terms = FreqDist() 
        for sent in LEMMAS:
            for num in range(1,4):
                grams = ngrams(sent, num)
                terms.update(grams)
        return terms
    
    sent     = [get_sentences(f) for f in filename]
    tagged   = [tok_and_tag(s) for s in sent]
    filtered = [filter_words(t) for t in tagged]
    lemmas   = [get_lemmas(f) for f in filtered]
    freqdist = [get_grams(l) for l in lemmas][0]
    return freqdist 

def make_vecs(fda, fdb):
    key_set = set(fda.keys())
    key_set.update(fdb.keys())
    va = {}
    vb = {}
    for k in key_set:
        va[k] = fda[k]
        vb[k] = fdb[k]
    return va, vb

def cosine(va, vb):
    numer = sum([(va[k] * vb[k]) for k in va.keys()])
    denom = np.sqrt(sum([va[k]*va[k] for k in va.keys()])) * np.sqrt(sum([vb[k]*vb[k] for k in va.keys()]))
    return numer/denom

def divide_work(FULL_LIST_OF_JK):
    n_cpus = mp.cpu_count()*1
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

def process_target(partial_list_of_jk, pipe):
    output = {}
    for jk in partial_list_of_jk:
        file = CORPUS_PATH + jk
        fd = get_freqdist([file])
        v1, v2 = make_vecs(fd, fd_resume)
        output[jk] = cosine(v1, v2)
    pipe.send(output)
    pipe.close()

fd_resume = get_freqdist([RESUME_FILE])

with psycopg2.connect(DB_CONNECTION) as conn:
    cur = conn.cursor()
    cur.execute("SELECT jk from cosine")
    already_processed = cur.fetchall()
    cur.close()

flattened = [i[0] for i in already_processed]
work_target = [j for j in os.listdir("./jobs/") if j not in flattened ]


pipes = []
processes = []
cos_dict = {}
if __name__ == '__main__':
    for job in divide_work(work_target):
        workslice = work_target[job[0]:job[0]+job[1]]
        pipe_recv, pipe_send = mp.Pipe(False)
        pipes.append(pipe_recv)
        p = mp.Process(target=process_target,
            args=(workslice,
            pipe_send))
        processes.append(p)    
        p.start()
    for i in range(len(processes)):
        cos_dict.update(pipes[i].recv())        
        processes[i].join()        

buffer = io.StringIO()
w = csv.writer(buffer)
w.writerows(cos_dict.items())
buffer.seek(0)

with psycopg2.connect(DB_CONNECTION) as conn:
    conn.set_client_encoding('UTF8')
    cur = conn.cursor()
    cur.copy_from(buffer, 'cosine', sep = ",")

