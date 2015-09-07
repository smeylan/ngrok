#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, subprocess, re, sys, itertools, codecs, gzip, glob, unicodedata, click, pandas, srilm, pdb, json, multiprocessing, time, tempfile, math, scipy
from zs import ZS
from scipy import stats

#ngrok library
class Worker(multiprocessing.Process): 
    def __init__(self,queue,myList):
        super(Worker, self).__init__()
        self.queue = queue
        self.myList = myList
        
    def run(self):
        for job in iter(self.queue.get, None): # Call until the sentinel None is returned
        	processGoogle(job['inputfile'], job['outputfile'] , job['yearbin'], job['quiet'], job['n'], job['earliest'], job['latest'], job['reverse'], job['strippos'], job['lower'])	
        	self.myList.append(job['inputfile'])

def processGoogleDirectory(inputdirectory, outputdirectory, yearbin, quiet, n, earliest, latest, reverse, strippos, lower):
	'''Parallelized, load-balanced execution of processGoogle, starting with the largest files'''
	start_time =  time.time()

	# Put the manager in charge of how the processes access the list
	mgr = multiprocessing.Manager()
	myList = mgr.list() 
    
	# FIFO Queue for multiprocessing
	q = multiprocessing.Queue()
    
	# Start and keep track of processes
	procs = []
	for i in range(24):
		p = Worker( q,myList )
		procs.append(p)
		p.start()
                
	files = glob.glob(inputdirectory+'*.gz') 
	filesizes = [(x, os.stat(x).st_size) for x in files]
	filesizes.sort(key=lambda tup: tup[1], reverse=True)
	
	# Add data, in the form of a dictionary to the queue for our processeses to grab    
	[q.put({"inputfile": file[0], "outputfile": os.path.join(outputdirectory, os.path.splitext(os.path.basename(file[0]))[0]+'.output'), 'yearbin': yearbin, "quiet": quiet, "n":n, "earliest":earliest, "latest":latest, "reverse":reverse, "strippos":strippos, "lower":lower}) for file in filesizes] #!!! limit the scope for testing
      
	#append none to kill the workers with poison pills		
	for i in range(24):
		q.put(None) #24 sentinels to kill 24 workers
        
	# Ensure all processes have finished and will be terminated by the OS
	for p in procs:
		p.join()     
        
	for item in myList:
		print(item)

	print('Done! processed '+str(len(myList))+' files; elapsed time is '+str(round(time.time()-startTime /  60., 5))+' minutes') 	


def processGoogle(inputfile, outputfile, yearbin, quiet, n, earliest, latest, reverse, strippos, lower):
	'''Cleans the raw ngram counts from the Google download site, removing POS, capitalization, and reversing the order of the ngram as necessary. Backwards compatible with process-google.py from ngrampy. '''
	#Adapted from ngrampy
	BUFSIZE = int(1e6) # We can allow huge buffers if we want...
	ENCODING = 'utf-8'
	LINE_N = n+3 # three extra columns		
	assert(latest > earliest)		

	prev_year,prev_ngram = None, None
	count = 0
	year2file = dict()
	part_count = None

	# build the table of unicode characters for cleaning the punctuation
	tbl = dict.fromkeys(i for i in xrange(sys.maxunicode)
		if unicodedata.category(unichr(i)).startswith('P'))

	# python is not much slower than perl if we pre-compile regexes

	#cleanup = re.compile(r"(_[A-Za-z\_\-]+)|(\")") # The old way -- delete tags and quotes
	line_splitter = re.compile(r"\n", re.U)
	cleanup_quotes = re.compile(r"(\")", re.U) # kill quotes
	#column_splitter = re.compile(r"[\s]", re.U) # split on tabs OR spaces, since some of google seems to use one or the other. 

	tag_match = re.compile(r"^(.+?)(_[A-Z\_\-\.\,\;\:]+)?$", re.U) # match a tag at the end of words (assumes 
	
	def tagify(x):
		"""
		Take a word with a tag ("man_NOUN") and give back ("man","NOUN") with "NA" if the tag or word is not there
		"""
		m = tag_match.match(x)
		if m:
			g = m.groups()

			word = (g[0] if g[0] is not None else "NA")
			tag  = (g[1] if g[1] is not None else "NA")
			return (word,tag)
			#if g[1] is None: return (g[0], "NA")
			#else:            return g
		else: return []

	def chain(args):
		a = []
		for x in args: a.extend(x)
		return a

	for f in glob.glob(inputfile):
		
		# Unzip and encode
		iff = gzip.open(f, 'r')
		for l in iff:	
			l = l.decode('utf-8')

			l = l.strip() ## To collapse case
			l = cleanup_quotes.sub("", l)   # remove quotes
			
			#parts = column_splitter.split(l)
			parts = l.split() # defaultly should handle splitting on whitespace, much friendlier with unicode
			
			# Our check on the number of parts -- we require this to be passed in (otherwise it's hard to parse)
			if len(parts) != LINE_N: 
				if not quiet: print "Wrong number of items on line: skipping ", l, parts, " IN FILE ", f
				continue # skip this line if its garbage NOTE: this may mess up with some unicode chars?
			#print parts	
			# parts[-1] is the number of books -- ignored here
			c = int(parts[-2]) # the count

			if ((int(parts[-3]) < earliest) or (int(parts[-3]) > latest)):
				#if record is outside the desired age range, advance in the for loop	
				continue
			if yearbin is 0:
				year  = 0 #all records are assigned to the same year
			else:
				year = int(int(parts[-3]) / yearbin) * yearbin # round the year
			
			ngram_ar = chain(map(tagify,parts[0:-3]))

			if not all([x == 'NA' for x in ngram_ar[1::2]]):
				#only count the cases without POS tags (in odd positions)
				continue

			if not all([x.count('_') < 2 for x in ngram_ar[0::2]]):
				#only count the cases that area actually words, not POS tags
				continue	

			#print ngram_ar
			#if all([x != "NA" for x in ngram_ar]): # Chuck lines that don't have all things tagged
			#else: continue
					
			#reverse the ngram	
			#reverse the order in blocks of 2, noting that the first word precedes its pos,
			#e.g. word1 pos1 pos2 word2 pos3 word3
			#only take the string, not the POS	
			if(reverse and strippos):			
				revOrder = range(0,n*2,2)[::-1]
			elif(reverse and not strippos):
				if(n == 1):
					revOrder = [0, 1]
				if(n == 2):	
					revOrder = [2, 3, 0, 1]
				if(n == 3):	
					revOrder = [4, 5, 2, 3, 0, 1]	
				if(n == 4):	
					revOrder = [6, 7, 4, 5, 2, 3, 0, 1]		
				if(n == 5):	
					revOrder = [8, 9 , 6, 7, 4, 5, 2, 3, 0, 1]			
			elif(not reverse and strippos):	
				#take [0] [0 2 4], [0 2 4 6] 
				revOrder = range(0,n*2,2)  
			elif(not reverse  and not strippos):		
				#take [0 1 2 3 4 5] #done
				revOrder = range(0, n*2)

			reorderedNgram = [ngram_ar[i] for i in revOrder]			

			reorderedNgram = [x for x in reorderedNgram if x != u'NA']
			if lower:
				ngram = [x.lower() for x in reorderedNgram]
			else:
				ngram = reorderedNgram
						
			ngram = filter(None,[remove_punctuation(x, tbl) for x in ngram])
			
			if len(ngram) != n: 
				if not quiet: print "Wrong number of items on line after removing punctuation: skipping ", l, parts, " IN FILE ", f
				continue		

			ngram = "\t".join(ngram)
			#if ngram =='&\t,\tL.Z.':
			#	pdb.set_trace()
			#if(reorderedNgram[0]=='_DET'):
				#pdb.set_trace()

			# output the current trigram if the current one is different
			if year != prev_year or ngram != prev_ngram:
				if prev_year is not None:
					#this creates the year file if it does not exist	
					if prev_year_s not in year2file: 
						year2file[prev_year_s] = open(outputfile+".%i"%prev_year, 'w', BUFSIZE)
					year2file[prev_year_s].write( "%s\t%i\n" % (prev_ngram.encode('utf-8'),count)  ) # write the preceding record to the year file TODO: This might should be unicode fanciness?
				
				prev_ngram = ngram
				prev_year  = year
				prev_year_s = str(prev_year)
				count = c
			else:
				count += c
			
			# And write the last line if we didn't already
			if not (year == prev_year and ngram == prev_ngram):
				if prev_year_s not in year2file: 
					year2file[prev_year_s] = open(outputfile+".%i"%prev_year, 'w', BUFSIZE)
				year2file[prev_year_s].write( "%s\t%i\n" % (prev_ngram.encode('utf-8'),count)  ) # write to the year file TODO: This might should be unicode fanciness?

		# Obligatory write of the last line, outside of iterating over the lines
		if prev_year_s not in year2file: 	
			year2file[prev_year_s] = open(outputfile+".%i"%prev_year, 'w', BUFSIZE)
		year2file[prev_year_s].write( "%s\t%i\n" % (prev_ngram.encode('utf-8'),count))
				
		iff.close()

	# And close everything
	[year2file[year].close() for year in year2file.keys()]	

def remove_punctuation(text, tbl):
	'''remove punctuation from UTF8 strings given a character table'''
	return text.translate(tbl)

def combineFiles(inputdirectory, outputfile):
	'''combines a set of text files into a single file; a wrapper for GNU cat'''
	print('Combining the cleaned files...')	
	catCommand = 'cat '+os.path.join(inputdirectory,'*.output.0')+' > '+outputfile
	subprocess.call(catCommand, shell=True)
	print('Done!')

def sortNgramFile(inputfile, outputfile):
	'''sorts an ngram file; basically a wrapper for GNU sort'''
	print('Sorting the combined file...')	
	sortCommand = 'env LC_ALL=C sort --compress-program=lzop '+inputfile+'  -o '+outputfile+' --parallel=24'
	subprocess.call(sortCommand, shell=True)
	print('Done!')

def collapseNgramFile(inputfile, outputfile):		
	'''After sorting the cleaned strings, there may be several prefixes that need to be collapsed because they are now the same. Given that records are ordered linearly, this means that we can just run through line by line and aggregate by prefix.'''
	print('Collapsing equivalent ngrams...')	
	iff = open(inputfile, 'r')		
	firstLine= iff.readline()
	simpleCount = 0 #just add in every case
	totalCount = 0 #aggregate count
	cachedCount = int(firstLine.split()[-1]) #highest count for an individual trigram
	cachedNgram = '\t'.join(firstLine.split()[0:-1])
	of = open(outputfile,'w')
	for l in iff:
		parts = l.split()
		ngram = '\t'.join(parts[0:-1])
		count = int(parts[-1])
		simpleCount += count
		#if the ngram isn't the same, print out the last trigram and add it to the aggregate count, and restart the count
		if ngram != cachedNgram:
			totalCount += cachedCount
			#print('Added to total '+cachedNgram+': '+str(cachedCount))
			of.write(cachedNgram+'\t'+str(cachedCount)+'\n')
			#restart the count
			cachedNgram = ngram
			cachedCount = count
		else:
			#if it is the same, compare, take the largest, and advance
			cachedCount += count
			#print('Increased '+cachedNgram+' count to : '+str(cachedCount))
	#obligate write of the final line
	of.write(cachedNgram+'\t'+str(cachedCount)+'\n')
	iff.close()
	of.close()
	print('Done!')
	
def makeLanguageModel(inputfile, outputfile, metadata, codec):
	'''Take the cleaned and sorted file and put it into ZS file'''		
	print('Building the language model...')
	zs_command = 'zs make \''+json.dumps(metadata)+'\' --codec='+codec+' '+inputfile + ' ' + outputfile 
	subprocess.call(zs_command, shell=True)

def reverseGoogleFile(inputfile, outputfile):
	'''Reverse the order of the ngram in a Google-formatted ngram file. Note that this is a different procedure than rearranging the ngram files that are output by AutoCorpus'''
	print('Reversing existing model')		
	iff = open(inputfile, 'r')
	off = open(outputfile, 'w')
	for l in iff:		
		l = l.replace('\n','').replace(' ','\t')
		strArray = l.split('\t')
			
		count = strArray.pop(-1)
		strArray.reverse()
		strArray.append(count) #move the count to the end, reverse ngram	
		off.write('\t'.join(strArray)+'\n')
	iff.close()
	off.close()
	print('Done!')

def checkForReversibleModel(lexSurpDir, n, direction):	
	'''Search for an equivalent model with the ngrams in the opposite order. It is much, much faster, to reverse the text file than to run through the entire processing stack'''
	if direction == 'forwards':
		oppositeDirection = 'backwards'
	elif direction == 'backwards':
		oppositeDirection = 'forwards'		
	return(glob.glob(os.path.join(lexSurpDir,str(n)+'gram-'+oppositeDirection+'-collapsed.txt')))

def getGoogleBooksLanguageModel(corpusSpecification, n, reverse):
	'''Metafunction to create a ZS language model from Google Ngram counts. Does a linear cleaning, merges the file into a single document, sorts it, collapses identical prefixes, and builds the ZS file.'''
	startTime = time.time()
	lexSurpDir = os.path.join(corpusSpecification['faststoragedir'], corpusSpecification['analysisname'],corpusSpecification['corpus'],corpusSpecification['language'],'00_lexicalSurprisal')
	direction = 'backwards' if reverse else 'forwards'
	
	zs_metadata = { 
	"corpus": corpusSpecification['corpus'],
	"langauge": corpusSpecification['language'],
	"n": n,
	"direction": direction
	}
	print zs_metadata
	
	print('Checking if there are appropriate cleaned text files to create lower-order language model...')

	reversibleModel = checkForReversibleModel(lexSurpDir, n, direction)
	collapsedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-collapsed.txt')

	if len(reversibleModel) > 0:		
		reverseGoogleFile(reversibleModel, collapsedFile)			
	else:	
		print('No higher-order models found. Cleaning the input files... grab a coffee, this may take some days')
		#!!! appropriate parallelization depends on how we choose to speed this up
		#!!! special attention to filenames in processGoogle
		#!!! unzip the files to the SSD for faster processing
		#clean			
		inputfile = os.path.join(scorpusSpecification['lowStorageDir'],corpusSpecification['corpus'],corpusSpecification['language'],str(n),{})
		cleanedFiles = os.path.join(corpusSpecification['slowStorageDir'],corpusSpecification['corpus'],corpusSpecification['language'],'3-processed',analysis, '*.output.0')
		processGoogle(inputfile,cleanedFiles, yearbin = 0, quiet=True, n=n, earliest=1800, latest=2012, reverse=reverse, strippos=True, lower=True)
			
		#combine
		combinedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'.txt')
		combineFiles(cleanedFiles, combinedFile)
		
		#sort
		sortedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-sorted.txt')
		sortNgramFile(combinedFile, sortedFile)

		#collapse
		collapseNgramFile(sortedFile, collapsedFile)
							
	#build the language model	
	zsFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'.zs')	
	makeLanguageModel(collapsedFile, zsFile, zs_metadata, codec="none")

	print('Done! Completed file is at '+zsFile+'; elapsed time is '+str(round(time.time()-startTime, 5))+' seconds') 


def makeDirectoryStructure(faststoragedir, analysisname, corpus, language):
	print('Creating directories for analyses at '+os.path.join(faststoragedir, analysisname, corpus, language)+'...')	

	corpusLanguagePath = os.path.join(faststoragedir, analysisname, corpus, language)				
	lexSurpDir = os.path.join(faststoragedir, analysisname,corpus,language,'00_lexicalSurprisal')
	sublexSurpDir = os.path.join(faststoragedir, analysisname,corpus,language,'01_sublexicalSurprisal')
	correlationsDir = os.path.join(faststoragedir, analysisname, corpus,language,'02_correlations')

	if not os.path.exists(corpusLanguagePath):
		os.makedirs(corpusLanguagePath)
	if not os.path.exists(lexSurpDir):
		os.makedirs(lexSurpDir)	
	if not os.path.exists(sublexSurpDir):
		os.makedirs(sublexSurpDir)
	if not os.path.exists(correlationsDir):
		os.makedirs(correlationsDir)
	print('Directories created!')
	return lexSurpDir, sublexSurpDir, correlationsDir


def analyzeCorpus(corpusSpecification, analysisname, faststoragedir, slowStorageDir):
	'''Conducts the analysis on a given dataset (corpus + language).'''
	
	corpus = corpusSpecification['corpus'] 
	language = corpusSpecification['language'] 
	n = corpusSpecification['order'] 
	print('Processing '+corpus+':'+language)

	lexSurpDir, sublexSurpDir, correlationsDir = makeDirectoryStructure(faststoragedir, analysisname, corpus, language)	

	if (corpus == 'GoogleBooks2012'):
		if (language == 'eng'):

			print('Checking if input files exist...')
			#!!! check if files extant; if not, then download
			
			print('Building language models...')
			# get backwards-indexed model of highest order (n)
			getGoogleBooksLanguageModel(corpusSpecification, n, reverse=True)
			# get forwards-indexed model of order n-1 (text file  built as a consequence)
			getGoogleBooksLanguageModel(corpusSpecification, n-1, reverse=False)
				#get unigrams to be able to take top N words in the analysis
			if n > 2:
				getGoogleBooksLanguageModel(corpusSpecification, 1, reverse=False)
		else:
			raise NotImplementedError

	elif(corpus == 'BNC'):
		if (language == 'eng'):
			
			print('Checking if input files exist...')
			#!!! check if file extant; if not, then download

			#!!! does buildZSfromPlaintext.py preserve unicode?
					
			print('Building language models')
			# get backwards-indexed model of highest order (n)
			getPlaintextLanguageModel(corpusSpecification, n, reverse=True, cleaningFunction='cleanLine_BNC')
			# get forwards-indexed model of order n-1 (text file  built as a consequence)
			getPlaintextLanguageModel(corpusSpecification, n-1, reverse=False, cleaningFunction='cleanLine_BNC')
			#get unigrams to be able to take top N words in the analysis
			if n > 2:
				getPlaintextLanguageModel(corpusSpecification, 1, reverse=False, cleaningFunction='cleanLine_BNC')
		else:
			raise NotImplementedError	


	#shared by all datasets once 	
	#!!! implement this
	#getMeanSurprisal.py
	'/shared_hd/corpora/OPUS/en_opus_wordlist.csv'
	#analyzeSurprisalCorrelations.py

def getPlaintextLanguageModel(corpusSpecification, n, reverse, cleaningFunction):	
	'''This metafunction produces a ZS langauge model from a large plaintext document using the program "ngrams" from the AutoCorpus Debian package to count the n-gram frequencies for a specified order (n). Example use: for producing a ZS file from the BNC.'''
	startTime = time.time()
	lexSurpDir = os.path.join(corpusSpecification['faststoragedir'], corpusSpecification['analysisname'],corpusSpecification['corpus'],corpusSpecification['language'],'00_lexicalSurprisal')
	direction = 'backwards' if reverse else 'forwards'

	zs_metadata = { 
	"corpus": corpusSpecification['corpus'],
	"langauge": corpusSpecification['language'],
	"n": n,
	"direction": direction
	}
	print zs_metadata
	
	tbl = dict.fromkeys(i for i in xrange(sys.maxunicode)
	if unicodedata.category(unichr(i)).startswith('P'))

	inputfile = os.path.join(scorpusSpecification['lowStorageDir'],corpusSpecification['corpus'],corpusSpecification['language'],str(n),{})
	cleanedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-cleaned.txt')
	countedFile= os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-counted.txt')
	countMovedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-countMoved.txt')
	sortedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-sorted.txt')
	collapsedFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'-collapsed.txt')
	zsFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'.zs')	

	cleanTextFile(inputfile, cleanedFile, cleaningFunction)
	countNgrams(cleanedFile, countedFile)
	rearrangeNgramFile(countedFile, countMovedFile, reverse)
	sortNgramFile(countMovedFile, sortedFile)
	os.system ("cp "+sortedFile+" "+collapsedFile) #this just copies it, so filenames are equivalent to the google procedure
	makeLanguageModel(collapsedFile, zsFile, metadata, 'none')

	print('Done! Completed file is at '+zsFile+'; elapsed time is '+str(round(time.time()-startTime, 5))+' seconds') 


def rearrangeNgramFile(inputfile, outputfile, reverse):
	print('Rearranging the ngrams...')
	iff = open(inputfile, 'r')
	off = open(outputfile, 'w')
	for l in iff:		
		l = l.replace('\n','').replace(' ','\t')
		strArray = l.split('\t')
		if (len(strArray) == 1):
			continue
		else:		
			if reverse:
				count = strArray.pop(0)
				strArray.reverse()
				strArray.append(count) #move the count to the end, reverse ngram	
			else:	
				strArray.append(strArray.pop(0)) #just move the count to the end
			off.write('\t'.join(strArray)+'\n')
	iff.close()
	off.close()


def marginalizeNgramFile(inputfile, outputfile, n, sorttype):
	'''collapse counts from inputfile for sequences of length n'''
	print('Marignalizing over counts from higher-order ngram file to produce counts of '+str(n)+'-grams')
	#this method is lossy-- not all bigrams present in the dataset will be here--but the counts are consistent with the higher order ngram file.	
	#!!!inputfile must be sorted for this to work. How do we confirm that it is sorted?	
	iff = open(inputfile, 'r')
	tf_path = os.path.join(os.path.dirname(inputfile),next(tempfile._get_candidate_names()))
	tf = open(tf_path, 'w')
	firstLine= iff.readline()
	cachedCount = int(firstLine.split()[-1]) #highest count for an individual trigram
	cachedNgram = '\t'.join(firstLine.split()[0:n]) #indices are non-inclusive, so N=1 	

	print('Collapsing counts...')
	for l in iff:
		parts = l.split()
		ngram = '\t'.join(parts[0:n])
		count = int(parts[-1])
		#if the ngram isn't the same, print out the last trigram and add it to the aggregate count, and restart the count
		if ngram != cachedNgram:
			#print('Added to total '+cachedNgram+': '+str(cachedCount))
			if (sorttype == 'numeric'):
				tf.write(str(cachedCount)+'\t'+cachedNgram+'\n')
			elif (sorttype == 'alphabetic'):
				tf.write(cachedNgram+'\t'+str(cachedCount)+'\n')
			#restart the count, for the next ngram
			cachedNgram = ngram
			cachedCount = count
		else:
			#if it is the same, add it to the aggregate count
			cachedCount += count
			#print('Increased '+cachedNgram+' count to : '+str(cachedCount))
	#obligate write of final cached value at the end                
	if (sorttype == 'numeric'):
		tf.write(str(cachedCount)+'\t'+cachedNgram+'\n')
	elif (sorttype == 'alphabetic'):
		tf.write(cachedNgram+'\t'+str(cachedCount)+'\n')
	iff.close()
	tf.close()

	print('Sorting new counts...')
	#then run sort on the output file
	if (sorttype == 'numeric'):
		os.system("sort -n -r "+tf_path+' > '+outputfile) # sorted by descending frequency
	elif (sorttype == 'alphabetic'):
		os.system("env LC_ALL=C sort "+tf_path+' > '+outputfile) # sorted alphabetically, suitable for putting into a ZS file         
	os.remove(tf_path)
	print('Done!')

def countNgrams(inputfile, outputfile, n):
	'''Produces an ngram count for a text file using the ngrams command from Autocorpus'''
	print('Counting the ngrams...')
	ngramsCommand = 'cat '+inputfile+' | /usr/bin/ngrams -n '+str(n)+' > '+outputfile
	subprocess.call(ngramsCommand, shell=True)


def cleanTextFile(inputfile, outputfile, cleaningFunction):
	'''Cleans a plaintext file line by line with the function specified in cleaningFunction'''
	print('Cleaning the plaintext file...')

	def cleanLine_BNC(l):
		return remove_punctuation(l.lower().decode('utf-8')).encode('utf-8')
	cleanLineOptions = {'cleanLine_BNC': cleanLine_BNC}

	iff = open(inputfile, 'r')
	off = open(outputfile,'w')

	for line in iff:		
		off.write(cleanLineOptions['cleaningFunction'](line))

	iff.close()
	off.close()



def getMeanSurprisal(backwards_zs_path, forwards_txt_path, unigram_txt_path, wordlist_csv, cutoff, outputfile):
	start_time = time.time()
	'''producing mean surprisal estimates given a backwards n-gram language model and a forwards text file (to be read into a hash table) for order n-1. Produces mean information content (mean log probability, weighted by the frequency of each context) as well as sublexical surprisal using Kneser-Ney smoothing on a list of words from an externally-provided wordlist (e.g. top 25k most frequent words in the corpus that are also in OPUS or Switchboard).'''

	print('Loading the backwards ZS file for order n...')
	backward_zs = ZS(backwards_zs_path, parallelism=0)

	print('Loading the forwards hash table for order n-1...')
	bigrams = {}
	with open(forwards_txt_path) as f:
		for line in f:	
			lineElements = line.split()
			key = '\t'.join(lineElements[0:2])+'\t'.encode('utf-8') 
			val = int(lineElements[2])
			bigrams[key] = val

	print('Loading unigram file...')		
	uni_sorted_file = pandas.read_table(unigram_txt_path)
	top_words = uni_sorted_file['word']

	print('Loading OPUS file...')
	def hasNumbers(inputString):
		return any(char.isdigit() for char in inputString)

	wordlist_DF = pandas.read_csv(wordlist_csv)
	wordlist = wordlist_DF['word'].astype('str').tolist()
	
	frequent_words = [w for w in top_words if w in wordlist][:25000]	
	print('Retrieving lexical surprisal estimates...')

	surprisalEstimates = [get_mean_surp(bigrams, backward_zs, w, cutoff) for w in frequent_words]

	df = pandas.DataFrame(surprisalEstimates)
	df.columns = ['word','mean_surprisal_weighted','mean_surprisal_unweighted','frequency','numContexts','retrievalTime']
	df.to_csv(outputfile, index=False)	
	print('Done! Completed file is at '+outputfile+'; elapsed time is '+str(round(time.time()-start_time /  60., 5))+' minutes') 

def getSublexicalSurprisals(inputfile, outputfile, n, srilmpath):
	'''get the probability of each word's letter sequence using the set of words in the language''' 	
	print('Retrieving sublexical surprisal estimates...')
	df = pandas.read_table(inputfile)
	if n != -1:
		df = df.iloc[0:min(n,len(df)-1)]		

	LM = trainSublexicalSurprisalModel(df['word'].astype('str'), order=5, smoothing='kn', smoothOrder=[3,4,5], interpolate=True, srilmPath=srilmpath)
	#!!! there is something hard coded about the temp file creation that is crashing the process
	df['ss']   = [getSublexicalSurprisal(word, LM, 5, 'letters', returnSum=True) for word in list(df['word'])]
	df.to_csv(outputfile, index=False)
	print('Done!')

def get_mean_surp(bigrams_dict,zs_file_backward, word, cutoff):	
	start_time = time.time()	
	total_freq = 0
	surprisal_total = 0
	num_context = 0
	unweightedSurprisal = 0
	for record in zs_file_backward.search(prefix=word+"\t"):
		r_split = record.decode('utf8').split("\t")
		tri_count = int(r_split[3])
		if tri_count >= cutoff:
			total_freq += tri_count
			context = r_split[2] + "\t" + r_split[1] + "\t"
			num_context += 1	
			total_context_freq = bigrams_dict[context.encode('utf-8')]	
			cond_prob = math.log(tri_count / float(total_context_freq))
			surprisal_total += (tri_count * cond_prob) #this is weighted by the frequency of this context
			unweightedSurprisal +=  cond_prob #this is not
		else:
			continue	
	stop_time = time.time()
	st = None if total_freq == 0 else surprisal_total / float(total_freq)
	uwst = None if num_context == 0 else unweightedSurprisal / float(num_context)
	return (word, st, uwst, total_freq, num_context, (stop_time-start_time))


def trainSublexicalSurprisalModel(wordList, order, smoothing, smoothOrder, interpolate, srilmPath):	
	''' Train an n-gram language model using a list of types 

		wordList: array or list of types in the language
		order: integer representing the highest order encoded in the language model
		smoothing: Smoothing technique: 'wb' or 'kn'
		smoothOrder: list of integers, indicating which orders to smooth
		interpolate: boolean, indicating whether to use interpolation or not

	'''

	# ensure that ngram-count is on the path. shouldn't need to do this from the command line	
	os.environ['PATH'] = os.environ['PATH']+':'+srilmPath
	
	#generate the relevant filenames
	timestr = str(time.strftime("%Y%m%d-%H%M%S"))
	if not os.path.exists('temp/'): #!!! we probably shouldn't write to temp in this way
		os.makedirs('temp/')
	basePath= os.getcwd() + '/temp/' +timestr + '_'
	typeFile = basePath + 'typeFile.txt'
	modelFile = basePath + smoothing + '.LM'

	# write the type inventory to the outfile
	outfile = open(typeFile, 'w')
	sentences=[' '.join(list(word)) for word in wordList]
	print >> outfile, '\n'.join(sentences)
	outfile.close()

	# train a model with smoothing on the outfile
	discounting = ' '.join([''.join(['-', smoothing,'discount', str(x)]) for x in smoothOrder])
	commandString = 'ngram-count -text '+typeFile+' -order ' + str(order) + ' ' + discounting + (' -interpolate' if interpolate else '') + ' -no-sos -lm ' + modelFile
	subprocess.call(commandString, shell=True)

	# load the language model and return it
	lm = srilm.LM(modelFile, lower=True)
	return(lm)

def getSublexicalSurprisal(targetWord, model, order, method, returnSum):		

	''' Get the sublexical surprisal for a word
		targetWord: type for which surprisal is calculated
		model: pysrilm LM object
		order: specify n of n-gram model. e.g. 1 for unigrams
		method: get probability of sounds or letters. 
				if sounds, input must be a list of phones
		returnSum: if true, return sum of surprisal values
				otherwise, return a list of surprisal values		
	'''

	if (method == 'sounds'):
		#throw an error if the variable word is not already a list
		word = targetWord + ['</s>']
		infoContent = list()		
	elif (method == 'letters'):
		word = re.sub('[0123456789\\-\\.\\,\\=]', '',str(targetWord))			
		if(len(word) == 0):
			return(None)
			#proceed to the next one
		else:
			word = list(word) + ['</s>']
			infoContent = list()
	
	for phoneIndex in range(len(word)):
		if(phoneIndex - order < 0):
			i = 0 #always want the start to be positive 
		else:
			i = phoneIndex - order + 1 					
		target=word[phoneIndex] 				 		
		preceding=word[i:phoneIndex][::-1] #pySRILM wants the text in reverse 		
		phonProb = model.logprob_strings(target,preceding)
		#print('Target: '+target,': preceding: '+' '.join(preceding)+'; prob:'+num2str(10**phonProb,5))
		infoContent.append(-1*phonProb)								
	if (all ([ x is not None for x in infoContent])):
		if returnSum:
			return(sum(infoContent))
		else:
			return(infoContent)	
	else:
		return(None)

def analyzeSurprisalCorrelations(lexfile, sublexfile, wordlist_csv, outfile):
	'''get correlations and plot the relationship between lexical and sublexical surprisal'''
	lex_DF = pandas.read_csv(lexfile)
	sublex_DF = pandas.read_csv(sublexfile)
	wordlist_DF = pandas.read_csv(wordlist_csv).drop_duplicates('word')	

	df = lex_DF.merge(sublex_DF, on='word').sort('frequency', ascending=False)	

	df_selected = wordlist_DF.merge(df, on='word').sort('frequency', ascending=False)
	df_selected['nchar'] = [len(x) for x in df_selected['word']]

	ssCor = scipy.stats.spearmanr(-1*df_selected['mean_surprisal_weighted'], df_selected['ss'])
	ncharCor = scipy.stats.spearmanr(-1*df_selected['mean_surprisal_weighted'], df_selected['nchar'])

	print ('number of words in analysis: ' + str(len(df_selected)) + ' types')
	print ("Spearman's rho for lexical and sublexical surprisal:" + str(ssCor))
	print ("Spearman's rho for lexical and character length:" + str(ncharCor))
	
	df_selected.to_csv(outfile, index=False)

def checkForMissingFiles(directory1, pattern1, directory2, pattern2):
	'''check which files from directory1 are not in directory2'''

	raw_files = glob.glob(os.path.join(directory1,pattern1))
	raw_filenames = [os.path.splitext(os.path.basename(x))[0] for x in raw_files]
	print('Directory 1 contains '+str(len(raw_filenames)) + ' files')


	processed_files = glob.glob(os.path.join(directory2,pattern2))
	processed_filenames = [os.path.splitext(os.path.basename(x))[0] for x in processed_files]
	processed_filenames = [os.path.splitext(os.path.basename(x))[0] for x in processed_filenames]

	if len(raw_filenames) != len(processed_filenames):
		print('Differeing number of raw and processed files')

		missing = []
		[missing.append(file) for file in raw_filenames if file not in processed_filenames]
		print('Missing:')
		print(missing)
	else:
		print('Same number of raw and processed files')	