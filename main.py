#!/usr/bin/python
# -*- coding: utf-8 -*-

#Top-level function. Specify the corpus, language, and order for all of the analyses

analysisName = "16Sept2015"
inputDir = "/shared_hd/corpora/" 
fastStorageDir = "/shared_ssd/ss/"
slowStorageDir = "/shared_hd2/ss/" 

import os, subprocess, re, sys, itertools, codecs, gzip, glob, unicodedata, click, pandas, srilm, pdb, json, multiprocessing, time, tempfile, math, scipy, warnings
#os.chdir('/home/stephan/python/ngrok') #necessary if running from the REPL
import ngrok
#reload(ngrok)


print('Checking for dependencies...')
dependencies = ['ngram','ngram-count','zs','gzrecover']
[ngrok.checkForBinary(d) for d in dependencies]

corporaToAnalyze = [
# {'corpus':'GoogleBooks2012',
# 	'language':'eng',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist':'/shared_hd/corpora/OPUS/en_opus_wordlist.csv'}
# 	,
# {'corpus':'GoogleBooks2012',
# 	'language':'spa',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd/corpora/OPUS/es_opus_wordlist.csv'},
# {'corpus':'GoogleBooks2012',
# 	'language':'fre',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd/corpora/OPUS/fr_opus_wordlist.csv'},
# {'corpus':'GoogleBooks2012',
# 	'language':'ger',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd/corpora/OPUS/de_opus_wordlist.csv'}
# {'corpus':'GoogleBooks2012',
# 	'language':'test',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd/corpora/OPUS/fr_opus_wordlist.csv'}
# {'corpus':'Google1T',
# 	'language':'SPANISH',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd/corpora/OPUS/es_opus_wordlist.csv'},
{'corpus':'Google1T',
	'language':'ENGLISH',
	'order':'3',
	'analysisname': analysisName,
	'inputdir':inputDir,
	'faststoragedir': fastStorageDir,
	'slowstoragedir': slowStorageDir,
	'wordlist': '/shared_hd/corpora/OPUS/en_opus_wordlist.csv'}	
]
# {'corpus': 'BNC',
# 	'language':'eng',
# 	'order':'3',
# 	'inputdir':inputDir,
# 	'analysisname': analysisName,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir}
# ]

if __name__ == '__main__':
    #[ngrok.downloadCorpus(x) for x in corporaToAnalyze]	    
    #[ngrok.validateCorpus(x) for x in corporaToAnalyze]    
    [ngrok.analyzeCorpus(x) for x in corporaToAnalyze]
