#!/usr/bin/python
# -*- coding: utf-8 -*-
#Top-level function. Specify the corpus, language, and order for all of the analyses

analysisName = "24Aug2015"
fastStorageDir = "/shared_ssd/ss/"
slowStorageDir = "/shared_hd/corpora/"

import os, subprocess, re, sys, itertools, codecs, gzip, glob, unicodedata, click, ngrok, ZS, pandas, srilm, pdb

print('Checking for dependencies...')
dependencies = ['srilm','autocorpus','zs']
def checkDependencies(command):
	test = subprocess.call(command)
	assert(test is not none)
[checkDependencies('which '+d) for d in dependencies]

corporaToAnalyze = [
{'corpus':'GoogleBooks2012',
	'langauge':'eng',
	'order':'3',
	'analysisName': analysisName,
	'fastStorageDir': fastStorageDir,
	'slowStorageDir': slowStorageDir},
{'corpus': 'BNC',
	'langauge':'eng',
	'order':'3',
	'analysisName': analysisName,
	'fastStorageDir': fastStorageDir,
	'slowStorageDir': slowStorageDir}
]

if __name__ == '__main__':
    [ngrok.analyzeCorpus(x, analysisName, storeIntermediateFiles, fastStorageDir, slowStorageDir) for x in corporaToAnalyze]
