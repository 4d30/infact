#!/usr/bin/env /usr/pkg/bin/python3.8

# Pretty ugly. Needs cleanup
#
# This script calculates tf & idf for all of the terms in all of the 
# documents in the directory specified. The process occurs over two
# loops. The first discovers and count all of the terms. The second
# counts the number of documents in which each term appears. 

import os
import multiprocessing as mp

import time
import datetime

import io
import psycopg2
import pickle

import pandas as pd
import numpy as np
import itertools

#from nltk import pos_tag
#from nltk.tokenize import word_tokenize, sent_tokenize, RegexpTokenizer
#from nltk.probability import FreqDist, Counter
#from nltk.corpus import stopwords, wordnet
#from nltk.stem import WordNetLemmatizer
#from nltk.util import ngrams

import jobtools

####################################
# TF functions
#def tag_nltk2wordnet(nltk_tag):
#    if nltk_tag.startswith('J'):
#        return wordnet.ADJ
#    elif nltk_tag.startswith('V'):
#        return wordnet.VERB
#    elif nltk_tag.startswith('N'):
#        return wordnet.NOUN
#    elif nltk_tag.startswith('R'):
#        return wordnet.ADV
#    else:          
#        return wordnet.NOUN
#
#def get_sentences(FILE):
#    raw = FILE.read().decode('utf8')
#    raw = raw.lower()
#    sent = sent_tokenize(raw)
#    return sent
#
#def get_lemmas(SENT):
#    tokenizer = RegexpTokenizer(r'\w+')
#    stop_words = list(stopwords.words("english"))
#    wnl = WordNetLemmatizer()
#    words = tokenizer.tokenize(SENT)
#    tagged = pos_tag(words)
#    filtered_words = [t for t in tagged if not t[0] in stop_words]
#    lemmas = [wnl.lemmatize(w[0], pos=tag_nltk2wordnet(w[1])) for w in filtered_words]
#    return lemmas
#
#def get_terms(LEMMAS):
#    terms = FreqDist()
#    for sent in LEMMAS:
#        for num in range(1,4):
#            grams = ngrams(sent, num)
#            terms.update(grams)
#    return terms


def count_terms_process_target(PATH, PARTIAL_LIST_OF_JK, TERMCOUNTER, PIPE):    
    counter_obj = count_terms_in_slice(PATH, PARTIAL_LIST_OF_JK, TERMCOUNTER)
    PIPE.send(counter_obj)
    PIPE.close()

####################################
# DF_Functions
def are_terms_in_doc(TERMS, FILENAME):
    termsindoc = FreqDist().update(jobtools.get_freqdist(FILENAME))
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

####################################
# General MP Functions
def count_terms_in_slice(PATH, PARTIAL_LIST_OF_JK, TERMCOUNTER):
    for jk in PARTIAL_LIST_OF_JK:
        filename = PATH + jk 
        TERMCOUNTER.update(jobtools.get_freqdist(filename))
    return TERMCOUNTER

#def divide_work(FULL_LIST_OF_JK):
#    n_cpus = mp.cpu_count()*1
#    job_length = np.floor(len(FULL_LIST_OF_JK)/n_cpus)
#    work_schedule = np.zeros((n_cpus,2), dtype = np.uint64)
#    for i in range(0, n_cpus):
#        distance = i*job_length
#        work_schedule[i,0] = distance
#        if i != (n_cpus - 1):
#            work_schedule[i,1] = job_length
#        else:
#            work_schedule[i,1] = len(FULL_LIST_OF_JK) - distance
#    return work_schedule

####################################
# Let's only target the most recent post from each employer
# within the last 30 days
def get_target_date(NUMBER_OF_DAYS_BEFORE_TODAY):
    return datetime.date.today() - datetime.timedelta(days = NUMBER_OF_DAYS_BEFORE_TODAY)

def get_work_target(DATAFRAME, TARGET_DATE, MAX_AGE_IN_DAYS):
    work_target = []
    for each in DATAFRAME.cmpid.unique():
        employer_listing = DATAFRAME[DATAFRAME.cmpid == each]
        most_recent = employer_listing[employer_listing['pub_date'] == employer_listing['pub_date'].max()].index
        for val in most_recent:
            pub_date = DATAFRAME.loc[val]['pub_date']
            n_days_befor_target = TARGET_DATE - datetime.timedelta(days = MAX_AGE_IN_DAYS) 
            if ( pub_date > n_days_befor_target and pub_date < TARGET_DATE):
                work_target.append(val)
            else:
                continue
    return work_target

def main():
    filepath = './jobs/'
    
    db_connection = "dbname=infact user=pgsql"
    with psycopg2.connect(db_connection) as conn:
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        jobmap = pd.read_sql_query("""SELECT jobmap.jk, jobmap.cmpid, pub_dates.pub_date FROM jobmap
                                    INNER JOIN pub_dates ON
                                    jobmap.jk = pub_dates.jk
                                    WHERE pub_dates.jk IS NOT NULL""",conn)
        cur.execute("SELECT cmp from cmpdir;")
        tuples = cur.fetchall()
        tmp_cmps = [ tuple_[0].lower() for tuple_ in tuples]
        cur.close()
    
    jobmap.set_index('jk', inplace = True)
    
    n_docs = len(jobmap)
    
    with psycopg2.connect(db_connection) as conn:
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()
        cur.execute("SELECT MAX(date_) FROM leaderboard")
        db_date = cur.fetchone()[0]
        for days in range(1,50):
            term_counter = Counter()
            start_time = time.time()
            target_date = get_target_date(days)
            if db_date is not None:
                if target_date <= db_date:
                    break
            print(target_date.strftime("%Y-%m-%d"))
            work_target = get_work_target(jobmap, target_date, 7)
            pipes = []
            processes = []
            ####################################
            # Identify and count terms
            if __name__ == '__main__':
                for job in jobtools.divide_work(work_target):
                    workslice = work_target[job[0]:job[0]+job[1]]
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
            print(f'Term Loop:\t {time.time() - start_time:.2f} sec')    
                
            pipes = []
            processes = []
            doc_counter = np.zeros((len(term_counter.keys())), dtype = np.uint64)
            start_time = time.time()
            ####################################
            # Count number of docs which contain each term found earlier
            if __name__ == '__main__':
                for job in jobtools.divide_work(work_target):
                    workslice = work_target[job[0]:job[0]+job[1]]
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
            
            print(f'Doc Loop:\t {time.time() - start_time:.2f} sec')    
            def logtf(LISTLIKE):
                if LISTLIKE > 0:
                    return 1 + np.log(LISTLIKE)
                else:
                    return 0
            df = pd.DataFrame.from_dict(term_counter, orient='index', dtype = int, columns = ['term_count'])
            df['doc_count'] = doc_counter
            df['tf_corpus'] = list(map(logtf,df['term_count'])) #1 + np.log(df['term_count']) #/sum(df['term_count'])
            df['idf'] = np.log(n_docs/df['doc_count'])
            df['tf-idf_corpus'] = df['tf_corpus']*df['idf']
            
            ####################################
            # Select only terms which appear in more than 55 documents
            df.index = list(map(lambda x: ' '.join(x), df.index))
            df.index.name = 'term'
    
            df['date'] = [target_date for i in df.index]
        
            buffer = io.StringIO()
            df_write = df.loc[:,['date','doc_count','tf-idf_corpus']]
            df_write['doc_count'] = df_write['doc_count'].astype(int)
            df_write.to_csv(buffer, index_label = 'term',  header = False)
            buffer.seek(0)
            start_time = time.time()
            try:
                cur.copy_from(buffer, 'leaderboard', sep = ",")
                conn.commit()
            except:
                print("Error: INSERT exception! Rolling back changes")
                conn.rollback()
            print(f'Insert Loop:\t {time.time() - start_time:.2f} sec\n')    
        cur.close()
    try:        
    #    print(df.sort_values(by='tf-idf_corpus', ascending=False)[0:50])
        df.sort_values(by='tf-idf_corpus', ascending=False).to_csv('./grams.csv')
    except:
        None
        
if __name__ == '__main__':
    main()
