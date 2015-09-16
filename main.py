#!/usr/bin/python
# -*- coding: utf-8 -*-

#Top-level function. Specify the corpus, language, and order for all of the analyses

analysisName = "16Sept2015"
inputDir = "/shared_hd/corpora/" 
fastStorageDir = "/shared_ssd/ss/"
slowStorageDir = "/shared_hd2/ss/" 

import ngrok
#reload(ngrok)

import os, subprocess, re, sys, itertools, codecs, gzip, glob, unicodedata, click, pandas, srilm, pdb, json, multiprocessing, time, tempfile, math, scipy, warnings

print('Checking for dependencies...')
dependencies = ['ngram','ngram-count','zs','gzrecover']
[ngrok.checkForBinary(d) for d in dependencies]

corporaToAnalyze = [
{'corpus':'GoogleBooks2012',
	'langauge':'eng',
	'order':'3',
	'analysisname': analysisName,
	'inputdirectory':inputDir,
	'faststoragedir': fastStorageDir,
	'slowstoragedir': slowStorageDir},
{'corpus':'GoogleBooks2012',
	'langauge':'spa',
	'order':'3',
	'analysisname': analysisName,
	'inputDir':inputDir,
	'faststoragedir': fastStorageDir,
	'slowstoragedir': slowStorageDir},
{'corpus': 'BNC',
	'langauge':'eng',
	'order':'3',
	'inputdir':inputDir,
	'analysisname': analysisName,
	'faststoragedir': fastStorageDir,
	'slowstoragedir': slowStorageDir}
]

if __name__ == '__main__':
    [ngrok.downloadCorpus(x) for x in corporaToAnalyze]	
    
    [ngrok.validateCorpus(x) for x in corporaToAnalyze]
    
    [ngrok.analyzeCorpus(x) for x in corporaToAnalyze]
