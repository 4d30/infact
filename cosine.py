#!/usr/bin/env /usr/pkg/bin/python3.8

import os
import multiprocessing as mp

import io
import csv
import psycopg2

import numpy as np

from nltk import pos_tag
from nltk.tokenize import sent_tokenize, RegexpTokenizer
from nltk.probability import FreqDist
from nltk.corpus import stopwords, wordnet
from nltk.stem import WordNetLemmatizer
from nltk.util import ngrams


def cosine(veca, vecb):
    """ calculates the cosine of the angle between two vectors """
    def mag(vector):
        return np.sqrt(sum([vector[k]*vector[k] for k in vector.keys()]))
    numer = sum([(veca[k] * vecb[k]) for k in veca.keys()])
    denom = mag(veca) * mag(vecb)
    return numer/denom

def divide_work(full_list_of_jk):
    """ creates an array to facilitate mp.  the indicies indicate
    how to split up the iterable.  
    [index of first jk, quantity of jks to process] """
    n_cpus = mp.cpu_count()*1
    job_length = np.floor(len(full_list_of_jk)/n_cpus)
    work_schedule = np.zeros((n_cpus,2), dtype = np.uint64)
    for i in range(0, n_cpus):
        distance = i*job_length
        work_schedule[i,0] = distance
        if i != (n_cpus - 1):
            work_schedule[i,1] = job_length
        else:
            work_schedule[i,1] = len(full_list_of_jk) - distance
    return work_schedule

def get_freqdist(filename):
    """ processes a job-description and returns a freqdist with 1,2,& 3-grams"""
    def tag_nltk2wordnet(nltk_tag):
        """ converts between nltk tags and wordNet tags """
        if nltk_tag.startswith('J'):
            return wordnet.ADJ
        if nltk_tag.startswith('V'):
            return wordnet.VERB
        if nltk_tag.startswith('N'):
            return wordnet.NOUN
        if nltk_tag.startswith('R'):
            return wordnet.ADV
        return wordnet.NOUN

    def get_sentences(file):
        """ converts file to a list of sentences """
        with open(file, 'r') as fdesc:
            raw = fdesc.read()
        raw = raw.lower()
        sent = sent_tokenize(raw)
        return sent

    def tok_and_tag(sent):
        """ breaks each sentence into tokens and then applies a POS tag """
        tokenizer = RegexpTokenizer(r'\w+')
        words = [tokenizer.tokenize(s) for s in sent]
        tagged = [pos_tag(w) for w in words]
        return tagged

    def filter_words(tagged):
        """ removes stop words """
        stop_words = list(stopwords.words("english"))
        filtered_words = []
        for tag in tagged:
            if len(tag) == 0:
                continue
            if tag[0] not in stop_words:
                filtered_words.append(tag)
        return filtered_words

    def get_lemmas(tagged):
        """ lemmatizes words with POS-tag attached """
        wnl = WordNetLemmatizer()
        lemmas = []
        for each in tagged:
            lemmas.append([wnl.lemmatize(w[0], pos=tag_nltk2wordnet(w[1])) for w in each])
        return lemmas

    def get_grams(lemmas):
        """ converts lemmatized sentences into a freqdist of 1,3-grams """
        terms = FreqDist()
        for sent in lemmas:
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
    """ makes vectors from two freqdist with differing keys """
    key_set = set(fda.keys())
    key_set.update(fdb.keys())
    veca = {}
    vecb = {}
    for k in key_set:
        veca[k] = fda[k]
        vecb[k] = fdb[k]
    return veca, vecb

def process_target(partial_list_of_jk, pipe):
    """ function to be sent to each cpu """
    output = {}
    fd_resume = get_freqdist([RESUME_FILE])
    for jobkey in partial_list_of_jk:
        file = CORPUS_PATH + jobkey
        freqdist = get_freqdist([file])
        vec1, vec2 = make_vecs(freqdist, fd_resume)
        output[jobkey] = cosine(vec1, vec2)
    pipe.send(output)
    pipe.close()

def write_to_mem(cos_dict):
    """ writes a dict to RAM for fast access """
    buffer = io.StringIO()
    writer = csv.writer(buffer)
    writer.writerows(cos_dict.items())
    buffer.seek(0)
    return buffer

def main():
    """ preprocesses files -> calculates cosine -> updates PGSQL table"""
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
            proc = mp.Process(target=process_target,
                args=(workslice,
                pipe_send))
            processes.append(proc)
            proc.start()
        for i in range(len(processes)):
            cos_dict.update(pipes[i].recv())
            processes[i].join()

    buffer = write_to_mem(cos_dict)

    with psycopg2.connect(DB_CONNECTION) as conn:
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        cur.copy_from(buffer, 'cosine', sep = ",")
        cur.close()

if __name__ == '__main__':
    DB_CONNECTION = "dbname=infact user=pgsql"
    RESUME_FILE = "./sample.txt"
    CORPUS_PATH = "./jobs/"
    main()
