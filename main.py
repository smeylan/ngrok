#!/usr/bin/python
# -*- coding: utf-8 -*-

#Top-level function. Specify the corpus, language, and order for all of the analyses

analysisName = "16Sept2015"
inputDir = "/shared_hd0/corpora/" 
fastStorageDir = "/shared_ssd/ss/"
slowStorageDir = "/shared_hd1/ss/" 

import os, subprocess, re, sys, itertools, codecs, gzip, glob, unicodedata, click, pandas, srilm, pdb, json, multiprocessing, time, tempfile, math, scipy, warnings
#os.chdir('/home/stephan/python/ngrok') #necessary if running from the REPL
import ngrok
#reload(ngrok)


print('Checking for dependencies...')
dependencies = ['ngram','ngram-count','zs','gzrecover']
[ngrok.checkForBinary(d) for d in dependencies]

corporaToAnalyze = [
# {'corpus':'GoogleBooks2012',
# 	'language':'eng-all',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist':'/shared_hd0/corpora/OPUS/2013_OPUS/en_2013.txt',
# 	'country_code': 'en'},
# {'corpus':'GoogleBooks2012',
# 	'language':'spa-all',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/es_2013.txt',
# 	'country_code': 'es'},
# {'corpus':'GoogleBooks2012',
# 	'language':'fre-all',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/fr_2013.txt',
# 	'country_code': 'fr'},
# {'corpus':'GoogleBooks2012',
# 	'language':'ger-all',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/de_2013.txt',
# 	'country_code': 'de'},
# {'corpus':'Google1T',
# 	'language':'ENGLISH',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/en_2013.txt',
# 	'country_code': 'en'},
# {'corpus':'Google1T',
# 	'language':'SPANISH',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/es_2013.txt',
# 	'country_code': 'es'},
# {'corpus':'Google1T',
# 	'language':'FRENCH',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/withPhonology/fr_2013.txt',
# 	'country_code': 'fr'},
# {'corpus':'Google1T',
# 	'language':'DUTCH',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/nl_2013.txt',
# 	'country_code': 'nl'},
# {'corpus':'Google1T',
# 	'language':'CZECH',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/cs_2013.txt',
# 	'country_code': 'cs'},
# {'corpus':'Google1T',
# 	'language':'ROMANIAN',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/ro_2013.txt',
# 	'country_code': 'ro'},
# {'corpus':'Google1T',
# 	'language':'POLISH',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/pl_2013.txt',
# 	'country_code': 'pl'},
# {'corpus':'Google1T',
# 	'language':'PORTUGUESE',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/pt_2013.txt',
# 	'country_code': 'pt-BR'},
 # {'corpus':'Google1T',
	# 'language':'ITALIAN',
	# 'order':'3',
	# 'analysisname': analysisName,
	# 'inputdir':inputDir,
	# 'faststoragedir': fastStorageDir,
	# 'slowstoragedir': slowStorageDir,
	# 'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/it_2013.txt',
	# 'country_code': 'it'},
{'corpus':'Google1T',
	'language':'SWEDISH',
	'order':'3',
	'analysisname': analysisName,
	'inputdir':inputDir,
	'faststoragedir': fastStorageDir,
	'slowstoragedir': slowStorageDir,
	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/sv_2013.txt',
	'country_code': 'sv'}]	
# {'corpus':'Google1T',
# 	'language':'GERMAN',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd0/corpora/OPUS/2013_OPUS/de_2013.txt',
# 	'country_code': 'de'},			
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



#corpusSpecification = {'corpus':'GoogleBooks2012',
# 	'language':'spa-all',
# 	'order':'3',
# 	'analysisname': analysisName,
# 	'inputdir':inputDir,
# 	'faststoragedir': fastStorageDir,
# 	'slowstoragedir': slowStorageDir,
# 	'wordlist': '/shared_hd/corpora/OPUS/es_opus_wordlist.csv'}


# ngrok.validateCorpus(corpusSpecification)
