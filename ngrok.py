#!/usr/bin/python
# -*- coding: utf-8 -*-

import os, subprocess, re, sys, itertools, codecs, gzip, glob, unicodedata, click, pandas, srilm, pdb, json, multiprocessing, time, tempfile, math, scipy, warnings, codecs
from zs import ZS
from scipy import stats

#ngrok library
#!!! make sure that the CLI commands are updated

class cgWorker(multiprocessing.Process):
    '''single-thread worker for parallelized cleanGoogle function'''  
    def __init__(self,queue,myList):
        super(cgWorker, self).__init__()
        self.queue = queue
        self.myList = myList
        
    def run(self):    	
        for job in iter(self.queue.get, None): # Call until the sentinel None is returned
        	try:
        		cleanGoogle(job['inputfile'], job['outputfile'], job['collapseyears'],job['filetype'])        
        	except ValueError:
        		print 'Problems encountered in cleaning '+job['inputfile']
			self.myList.append(job['inputfile'])

class pgWorker(multiprocessing.Process): 
    '''single-thread worker for parallelized processGoogle function''' 
    def __init__(self,queue,myList):
        super(pgWorker, self).__init__()
        self.queue = queue
        self.myList = myList
        
    def run(self):
        for job in iter(self.queue.get, None): # Call until the sentinel None is returned
        	processGoogle(job['inputfile'], job['outputfile'] , job['yearbin'], job['quiet'], job['n'], job['earliest'], job['latest'], job['reverse'], job['strippos'], job['lower'])	
        	self.myList.append(job['inputfile'])

def cleanGoogleDirectory(inputdir, outputdir, collapseyears):
	'''Parallelized, load-balanced execution of cleanGoogle, starting with the largest files'''
	start_time =  time.time()

	# Put the manager in charge of how the processes access the list
	mgr = multiprocessing.Manager()
	myList = mgr.list() 
    
	# FIFO Queue for multiprocessing
	q = multiprocessing.Queue()
    
	# Start and keep track of processes
	procs = []
	for i in range(12):
		p = cgWorker( q,myList )
		procs.append(p)
		p.start()
	              
	files = glob.glob(os.path.join(inputdir,'*.gz')) 
	if len(files) > 0:
		print('File type is gz')	
		filetype = 'gz'
	else:
		files = glob.glob(os.path.join(inputdir,'*.bz2'))
		if len(files) > 0:
			print('File type is bz2')	
			filetype = 'bz2'	
		else:
			raise ValueError('No files found')		
		
	filesizes = [(x, os.stat(x).st_size) for x in files]
	filesizes.sort(key=lambda tup: tup[1], reverse=True)
	
	extension = '.yc' if collapseyears else '.output'
	# Add data, in the form of a dictionary to the queue for our processeses to grab    
	[q.put({"inputfile": file[0], "outputfile": os.path.join(outputdir, os.path.splitext(os.path.basename(file[0]))[0]+extension),"collapseyears": collapseyears, 'filetype':filetype}) for file in filesizes] 
      
	#append none to kill the workers with poison pills		
	for i in range(12):
		q.put(None) #24 sentinels to kill 24 workers
        
	# Ensure all processes have finished and will be terminated by the OS
	for p in procs:
		p.join()     
        
	for item in myList:
		print(item)

	print('Done! processed '+str(len(myList))+' files; elapsed time is '+str(round(time.time()-start_time /  60., 5))+' minutes') 	

def processGoogleDirectory(inputdir, outputdir, yearbin, quiet, n, earliest, latest, reverse, strippos, lower):
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
		p = pgWorker( q,myList )
		procs.append(p)
		p.start()
                
	files = glob.glob(os.path.join(inputdir,'*.gz')) 
	filesizes = [(x, os.stat(x).st_size) for x in files]
	filesizes.sort(key=lambda tup: tup[1], reverse=True)
	
	# Add data, in the form of a dictionary to the queue for our processeses to grab    
	[q.put({"inputfile": file[0], "outputfile": os.path.join(outputdir, os.path.splitext(os.path.basename(file[0]))[0]+'.output'), 'yearbin': yearbin, "quiet": quiet, "n":n, "earliest":earliest, "latest":latest, "reverse":reverse, "strippos":strippos, "lower":lower}) for file in filesizes] 
      
	#append none to kill the workers with poison pills		
	for i in range(24):
		q.put(None) #24 sentinels to kill 24 workers
        
	# Ensure all processes have finished and will be terminated by the OS
	for p in procs:
		p.join()     
        
	for item in myList:
		print(item)

	print('Done! processed '+str(len(myList))+' files; elapsed time is '+str(round(time.time()-start_time /  60., 5))+' minutes') 	


def collapseNgrams(inputfile, outputfile):
	'''aggregate across dates from a google-formatted ngram file'''
	bufsize = 10000000
	print('Collapsing years...')
	iff = open(inputfile, 'r')
	off = open(outputfile, 'w')	
	firstLine ='\n' #handle any lines that are blank at the beginning of the text
	#need to confirm that there is anything in the file
	while firstLine == '\n' or firstLine == '':
		firstLine = iff.readline()
	
	lineSplit = firstLine.split('\t')
	prev_ngram = lineSplit[0]
	if len(lineSplit) == 3:
		ncols = 3
		print('3 tab-delineated columns, assuming first is the ngram, second is the token and the third the context count')
		cached_count = int(lineSplit[1])
		cached_context_count = int(lineSplit[2])				
	elif len(lineSplit) == 2:
		print('2 tab-delineated columns, assuming first is the ngram, second is the token count')	
		cached_count = int(lineSplit[-1])		
		ncols = 2		
	rows =[]

	for c,l in enumerate(iff):
		line = l.split('\t')
		if len(line) != ncols:			
			print 'Mismatch in line length and ncols, line was '+line
			break
		ngram = line[0]
		count = int(line[1])
		if ncols == 3: 
			context_count = int(line[2])
				
		if(ngram != prev_ngram): #new ngram, write out the cached one			
			#after appending row to the buffer, reset the storage
			if ncols == 3:
				rows.append('\t'.join([prev_ngram, str(cached_count), str(cached_context_count) ]))
			elif ncols == 2:	
				rows.append('\t'.join([prev_ngram, str(cached_count)]))

			prev_ngram = ngram
			cached_count = count
			if ncols == 3:				
				cached_context_count = context_count
		else:
			cached_count += count
			if ncols == 3:
				cached_context_count += context_count

		if c % bufsize == 0:	
			off.write('\n'.join(rows)+'\n')
			rows =[] 
	if ncols == 3:
		rows.append('\t'.join([prev_ngram, str(cached_count), str(cached_context_count)])) # catch the last record		
	elif ncols == 2:
		rows.append('\t'.join([prev_ngram, str(cached_count)])) # catch the last record		

	off.write('\n'.join(rows)+'\n')	#catch any records since the last buffered write						 	
	iff.close()
	off.close()
	print('Finished collapsing years, output in file '+str(outputfile))

def cleanGoogle(inputfile, outputfile, collapseyears, filetype):
	'''Clean google trigram file. This is a highly streamlined version of process google that finds only non POS-tagged lines, with no punctuation, and makes them lowercase, using grep to find lines without punctuation (including _, which excludes lines with POS tags) and perl to lowercase the string, while maintaining the unicode encoding. If collapseyears is true, combine the year counts into a single record'''	
	if collapseyears:
		tempfile = inputfile+'_temp'
		if filetype == 'gz':
			cleanGoogleCommand = "zcat "+inputfile+" | LC_ALL=C grep -v '[[:punct:]]' | perl -CSD -ne 'print lc' > "+tempfile
		elif filetype == 'bz2':
			cleanGoogleCommand = "bzcat "+inputfile+" | LC_ALL=C grep -v '[[:punct:]]' | perl -CSD -ne 'print lc' > "+tempfile	
		os.system(cleanGoogleCommand)
		if os.stat(tempfile).st_size > 0 :	
			collapseNgrams(tempfile, outputfile) # this means that there are separate records for lowercase and uppercase items
		else:
			'Temp file has no content; safe to remove.'	
		os.remove(tempfile)
	else:	
		if filetype == 'gz':
			cleanGoogleCommand = "zcat "+inputfile+" | LC_ALL=C grep -v '[[:punct:]]' | perl -CSD -ne 'print lc' > "+outputfile
		elif filetype == 'bz2':
			cleanGoogleCommand = "bzcat "+inputfile+" | LC_ALL=C grep -v '[[:punct:]]' | perl -CSD -ne 'print lc' > "+outputfile
		os.system(cleanGoogleCommand)


def processGoogle(inputfile, outputfile, yearbin, quiet, n, earliest, latest, reverse, strippos, lower):
	'''Cleans the raw ngram counts from the Google download site, removing POS, capitalization, and reversing the order of the ngram as necessary. Backwards compatible with process-google.py from ngrampy. '''
	#Adapted from ngrampy
	BUFSIZE= int(1e6) # We can allow huge buffers if we want...
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

def combineFiles(inputdir, pattern, outputfile):
	'''combines a set of text files in directory with filenames terminating with pattern into a single file; a wrapper for GNU cat'''
	print('Combining the cleaned files...')	
	catCommand = 'cat '+os.path.join(inputdir,pattern)+' > '+outputfile
	subprocess.call(catCommand, shell=True)
	print('Done!')

def sortNgramFile(inputfile, outputfile):
	'''sorts an ngram file; basically a wrapper for GNU sort'''
	print('Sorting the combined file...')	
	sortCommand = 'env LC_ALL=C sort --compress-program=lzop '+inputfile+'  -o '+outputfile+' --parallel=24'
	subprocess.call(sortCommand, shell=True)
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
		strArray = l.split('\t')		
		if len(strArray) > 0: #this cleans any empty lines that are produced by the cleaning process
			ngram = strArray[0].split(' ')
			if len(ngram) > 0: #only retain proper ngrams
				strArray[0] = ' '.join(ngram[::-1])
				off.write('\t'.join(strArray)+'\n')
	iff.close()
	off.close()
	print('Done!')

def deriveFromHigherOrderModel(intermediatefiledir, n, direction):
	'''Search for a pre-computed model from which the desired counts can be derived either through reversing or marginalization'''
	if direction == 'forwards':
		oppositeDirection = 'backwards'
	elif direction == 'backwards':
		oppositeDirection = 'forwards'		
	
	#first look for a model in the same direction that is larger than the desired n

	availableModels = glob.glob(os.path.join(intermediatefiledir,'*'+direction+'-collapsed.txt'))
	modelOrders = [os.path.basename(x)[0] for x in availableModels if x > int(n)]
	if len(modelOrders) > 0:
		print 'Higher order model of same direction found; will marginalize counts...'
		NtoUse = min(modelOrders)
		inputfile = os.path.join(intermediatefiledir,str(NtoUse)+'gram-'+direction+'-collapsed.txt')
		outputfile = os.path.join(intermediatefiledir,str(n)+'gram-'+direction+'-collapsed.txt')
		#!!! sort before marginalization! may be okay
		marginalizeNgramFile(inputfile, outputfile, n, 'alphabetic')
		return(outputfile)
	else: #no models in the same direction, may need to reverse one
		availableModels = glob.glob(os.path.join(intermediatefiledir,'*'+oppositeDirection+'-collapsed.txt'))	 #look for ones of the opposite direction		
		modelOrders = [int(os.path.basename(x)[0]) for x in availableModels if x > int(n)]

		if len(modelOrders) > 0: # if there is at least one higher-order opposite-direction model
			print 'Higher order model of different direction found; will reverse, sort, and marginalize'
			NtoUse = min(modelOrders)						

			#reverse it-- the higher order model MUST be reversed before marginalization, or some low frequency trigrams are lost
			startingModel = os.path.join(intermediatefiledir,str(NtoUse)+'gram-'+oppositeDirection+'-collapsed.txt')
			desiredDirectionStartingFile = os.path.join(intermediatefiledir,str(NtoUse)+'gram-'+direction+'-combined.txt')
			reverseGoogleFile(startingModel, desiredDirectionStartingFile)

			#sort it
			sortedFile = os.path.join(intermediatefiledir,str(n)+'gram-'+direction+'-sorted.txt')
			sortNgramFile(desiredDirectionStartingFile, sortedFile)

			#marginalize it							
			marginalizedfile = os.path.join(intermediatefiledir,str(n)+'gram-'+direction+'-marginalized.txt')
			marginalizeNgramFile(sortedFile,marginalizedfile, n, 'alphabetic')
						
			collapsedFile = os.path.join(intermediatefiledir,str(n)+'gram-'+direction+'-collapsed.txt')

			os.system('cp '+marginalizedfile+' '+collapsedFile)
			return(collapsedFile)
		else:
			print 'No appropriate models found, proceeding to cleaning the source trigrams.'
			return(None)


def getGoogleBooksLanguageModel(corpusSpecification, n, reverse, collapseyears, filetype):
	'''Metafunction to create a ZS language model from Google Ngram counts. Does a linear cleaning, merges the file into a single document, sorts it, collapses identical prefixes, and builds the ZS file.'''
	startTime = time.time()
	lexSurpDir = os.path.join(corpusSpecification['faststoragedir'], corpusSpecification['analysisname'],corpusSpecification['corpus'],corpusSpecification['language'],'00_lexicalSurprisal')

	direction = 'backwards' if reverse else 'forwards'
	if not collapseyears: #keeping dates is too large to keep the intermediate files on the ssd			
		intermediateFileDir = os.path.join(corpusSpecification['slowstoragedir'],corpusSpecification['corpus'],corpusSpecification['language'])
	else:
			intermediateFileDir	= lexSurpDir
	
	zs_metadata = { 
		"corpus": corpusSpecification['corpus'],
		"language": corpusSpecification['language'],
		"n": n,
		"direction": direction
	}
	print zs_metadata
	
	print('Checking if there are appropriate cleaned text files to create lower-order language model...')
	tryHigher = deriveFromHigherOrderModel(intermediateFileDir, n, direction)	
	
	if tryHigher is not None:
		print('Derived model from higher order model, results are at '+str(tryHigher))
		collapsedfile = tryHigher
	else:	
		print('No higher-order or reversible models found. Cleaning the input files... If n > 3 and the language is English, this is a good time to grab a coffee, this will take a few hours.')
		
		#find only lines without POS tags and make them lowercase
		inputdir = os.path.join(corpusSpecification['inputdir'],corpusSpecification['corpus'],corpusSpecification['language'],str(n))
		outputdir = os.path.join(corpusSpecification['slowstoragedir'],corpusSpecification['analysisname'], corpusSpecification['corpus'],corpusSpecification['language'],str(n)+'-processed')	
	
		combinedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-combined.txt')				
		if collapseyears:
			cleanFileProp = checkForMissingFiles(inputdir, '*.'+filetype, outputdir, '*.yc')	
			if cleanFileProp < .2:
				cleanGoogleDirectory(inputdir,outputdir, collapseyears)
				checkForMissingFiles(inputdir, '*.'+ filetype, outputdir, '*.yc')	
			combineFiles(outputdir, '*.yc', combinedfile)	

		else:	
			cleanFileProp = checkForMissingFiles(inputdir, '*.'+filetype, outputdir, '*.output')
			if cleanFileProp < .2:
				cleanGoogleDirectory(inputdir,outputdir, collapseyears)
				checkForMissingFiles(inputdir, '*.'+filetype, outputdir, '*.output')
			combineFiles(outputdir, '*.output', combinedfile)		

		#reverse if specified
		if reverse:
			reversedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-reversed.txt')
			reverseGoogleFile(combinedfile, reversedfile)
			fileToSort = reversedfile
		else:	
			fileToSort = combinedfile
		
		#sort it	
		sortedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-sorted.txt')
		sortNgramFile(fileToSort, sortedfile)		

		#collapse after the sorting: this deals with different POS treatments 
		collapsedfile = os.path.join(intermediateFileDir,str(n)+'gram-'+direction+'-collapsed.txt')
		collapseNgrams(sortedfile, collapsedfile)
							
	#build the language model	
	zsFile = os.path.join(lexSurpDir,str(n)+'gram-'+direction+'.zs')	
	makeLanguageModel(collapsedfile, zsFile, zs_metadata, codec="none")

	print('Done! Completed file is at '+zsFile+'; elapsed time is '+str(round(time.time()-startTime, 5))+' seconds') 
	return(zsFile)

def makeDirectoryStructure(faststoragedir, slowstoragedir, analysisname, corpus, language, n):		
	print('Creating fast storage directory at '+os.path.join(faststoragedir, analysisname, corpus, language)+'...')	

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
	print('Fast directories created!')

	processedDir = os.path.join(slowstoragedir, analysisname, corpus, language)
	print('Creating slow storage directory at '+processedDir+'...')	
	if not os.path.exists(processedDir):
		os.makedirs(processedDir)

	#create directories for all n, n-1, 1	
	ordersToMake = [n, n-1, 1]
	for i in ordersToMake:
		pathToMake = os.path.join(processedDir, str(i)+'-processed')
		if not os.path.exists(pathToMake):
			os.makedirs(pathToMake)

	return lexSurpDir, sublexSurpDir, correlationsDir, processedDir


def analyzeCorpus(corpusSpecification):
	'''Conducts the analysis on a given dataset (corpus + language).'''	
	corpus = corpusSpecification['corpus'] 
	language = corpusSpecification['language'] 
	n = corpusSpecification['order'] 
	print('Processing '+corpus+':'+language)

	lexSurpDir, sublexSurpDir, correlationsDir, processedDir = makeDirectoryStructure(corpusSpecification['faststoragedir'], corpusSpecification['slowstoragedir'], corpusSpecification['analysisname'], corpusSpecification['corpus'], corpusSpecification['language'], int(corpusSpecification['order']))	

	if (corpus == 'GoogleBooks2012'):
		if (language in ('eng', 'spa', 'fre','ger','test')):					
			print('Checking if input files exist...')			
			
			print('Building language models...')
			# get backwards-indexed model of highest order (n)
			backwardsNmodel = getGoogleBooksLanguageModel(corpusSpecification, int(n), reverse=True, collapseyears=True, filetype='gz')
			# get forwards-indexed model of order n-1 (text file  built as a consequence)
			forwardsNminus1model = getGoogleBooksLanguageModel(corpusSpecification, int(n)-1, reverse=False, collapseyears=True, filetype='gz')				
		else:
			raise NotImplementedError		
	elif(corpus == 'Google1T'):
		if (language in ('SPANISH','ENGLISH')):
			backwardsNmodel = getGoogleBooksLanguageModel(corpusSpecification, int(n), reverse=True, collapseyears=True, filetype='bz2')
			forwardsNminus1model = getGoogleBooksLanguageModel(corpusSpecification, int(n)-1, reverse=False, collapseyears=True, filetype='bz2')
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
	
	#build the unigram count list		
	forwardBigramPath = os.path.join(lexSurpDir, '2gram-forwards-collapsed.txt')
	unigramCountFilePath = os.path.join(lexSurpDir, 'unigram_list.txt')
	marginalizeNgramFile(forwardBigramPath, unigramCountFilePath, 1, 'numeric') 	
	
	print('Getting mean lexical surprisal estimates for types in the langauge...')
	forwardsNminus1txt = os.path.join(lexSurpDir,str(int(n)-1)+'gram-forwards-collapsed.txt')

	lexfile = os.path.join(lexSurpDir, 'opus_meanSurprisal25k.csv')	
	getMeanSurprisal(backwardsNmodel, forwardsNminus1txt, unigramCountFilePath,corpusSpecification['wordlist'], 0,lexfile)
	#what wordlist is being used in getting the mean surprisal numbers?
	#take the top 25k words? or OPUS for each language?    


	print('Getting sublexical surprisal estimates for types in the language...')
	numberOfTypesInModel = 200000
	sublexFilePath = os.path.join(sublexSurpDir, str(numberOfTypesInModel)+'_sublex.csv')
	getSublexicalSurprisals(unigramCountFilePath, sublexFilePath, numberOfTypesInModel)
	
	#this should produce the sublex
	outfile = os.path.join(correlationsDir,'opus_meanSurprisal25k_200ksublex.csv')
	analyzeSurprisalCorrelations(lexfile, sublexFilePath, corpusSpecification['wordlist'], outfile)
		

def getPlaintextLanguageModel(corpusSpecification, n, reverse, cleaningFunction):	
	'''This metafunction produces a ZS language model from a large plaintext document using the program "ngrams" from the AutoCorpus Debian package to count the n-gram frequencies for a specified order (n). Example use: for producing a ZS file from the BNC.'''
	startTime = time.time()
	lexSurpDir = os.path.join(corpusSpecification['faststoragedir'], corpusSpecification['analysisname'],corpusSpecification['corpus'],corpusSpecification['language'],'00_lexicalSurprisal')
	direction = 'backwards' if reverse else 'forwards'

	zs_metadata = { 
	"corpus": corpusSpecification['corpus'],
	"language": corpusSpecification['language'],
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
	#!!! marginalizer needs to handle tab and space separated input	
	#!!! combining context counts doesn't take into account that the same contexts might be used in both cases
	iff = open(inputfile, 'r')
	tf_path = os.path.join(os.path.dirname(inputfile),next(tempfile._get_candidate_names()))
	tf = open(tf_path, 'w')
		
	firstLine ='\n' #handle any lines that are blank at the beginning of the text
	while firstLine == '\n':
		firstLine = iff.readline()

	linesplit = firstLine.split('\t')
	cachedNgram = ' '.join(linesplit[0].split(' ')[0:n])
	cachedCount = int(linesplit[1]) 
	if len(linesplit)== 3:
		cachedContextCount = int(linesplit[2]) 
		ncols = 3
	else:
		ncols = 2

	print('Collapsing counts...')
	for l in iff:
		parts = l.split('\t')
		ngram = ' '.join(parts[0].split(' ')[0:n])		
		count = int(parts[1])
		if ncols == 3:
			contextCount = int(parts[2])					
		#if the ngram isn't the same, print out the last trigram and add it to the aggregate count, and restart the count
		if ngram != cachedNgram:
			#print('Added to total '+cachedNgram+': '+str(cachedCount))
			if (sorttype == 'numeric'):
				tf.write(str(cachedCount)+'\t'+cachedNgram+'\n')
			elif (sorttype == 'alphabetic'):
				if ncols == 3:	
					tf.write(cachedNgram+'\t'+str(cachedCount)+'\t'+str(cachedContextCount)+'\n')
				elif ncols == 2:
					tf.write(cachedNgram+'\t'+str(cachedCount)+'\n')	

			#restart the count, for the next ngram
			cachedNgram = ngram
			cachedCount = count
			if ncols == 3:
				cachedContextCount = contextCount
		else:
			#if it is the same, add it to the aggregate count
			cachedCount += count
			if ncols == 3:
				cachedContextCount += contextCount
			#print('Increased '+cachedNgram+' count to : '+str(cachedCount))
	#obligate write of final cached value at the end                
	if ncols == 3:	
		tf.write(cachedNgram+'\t'+str(cachedCount)+'\t'+str(cachedContextCount)+'\n')
	elif ncols == 2:
		tf.write(cachedNgram+'\t'+str(cachedCount)+'\n')	
	iff.close()
	tf.close()

	print('Sorting new counts...')
	#then run sort on the output file
	if (sorttype == 'numeric'):
		os.system("sort -n -r "+tf_path+' > '+outputfile) # sorted by descending frequency
		addCommand = "sed -i '1s/^/count\\tword\\n/' " # add labels, do this post hoc so we can sort the file
		os.system(addCommand + outputfile)
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
	f = codecs.open(forwards_txt_path, encoding='utf-8')
	for line in f:
		lineElements = line.split('\t')
		if len(lineElements) > 1:			
			key = lineElements[0]+u' ' 						
			val = int(lineElements[1])
			bigrams[key] = val
		else:
			pdb.set_trace()

	print('Loading unigram file...')		
	uni_sorted_file = pandas.read_table(unigram_txt_path, encoding='utf-8')
	top_words = uni_sorted_file['word']

	print('Loading OPUS file...')	
	def hasNumbers(inputString):
		return any(char.isdigit() for char in inputString)
	
	wordlist_DF = pandas.read_csv(wordlist_csv, encoding='utf-8')
	wordlist = wordlist_DF['word'].tolist()
	
	frequent_words = [w for w in top_words[0:30000] if w in wordlist][:25000]	
	print('Retrieving lexical surprisal estimates...')
	surprisalEstimates = [get_mean_surp(bigrams, backward_zs, w, cutoff) for w in frequent_words]

	df = pandas.DataFrame(surprisalEstimates)
	df.columns = ['word','mean_surprisal_weighted','mean_surprisal_unweighted','frequency','numContexts','retrievalTime']
	df.to_csv(outputfile, index=False, encoding='utf-8')	
	print('Done! Completed file is at '+outputfile+'; elapsed time is '+str(round(time.time()-start_time /  60., 5))+' minutes') 

def getSublexicalSurprisals(inputfile, outputfile, n):
	'''get the probability of each word's letter sequence using the set of words in the language''' 	
	print('Retrieving sublexical surprisal estimates...')
	df = pandas.read_table(inputfile)
	if n != -1:
		df = df.iloc[0:min(n,len(df)-1)]		

	LM = trainSublexicalSurprisalModel(df['word'].astype('str'), order=5, smoothing='kn', smoothOrder=[3,4,5], interpolate=True)
	#!!! there is something hard coded about the temp file creation that is crashing the process
	df['ss']   = [getSublexicalSurprisal(word, LM, 5, 'letters', returnSum=True) for word in list(df['word'])]
	df.to_csv(outputfile, index=False, encoding='utf-8')
	print('Done!')

def get_mean_surp(bigrams_dict,zs_file_backward, word, cutoff):	
	start_time = time.time()	
	total_freq = 0
	surprisal_total = 0
	num_context = 0
	unweightedSurprisal = 0	
	searchTerm = word+u" " #need a trailing space
	print 'Retrieving context probabilities for '+searchTerm	
	for record in zs_file_backward.search(prefix=searchTerm.encode('utf-8')):
		r_split = record.decode("utf-8").split(u"\t")
		ngram = r_split[0].split(u' ')
		#print r_split[0]
		count = int(r_split[1])
		if count >= cutoff:
			total_freq += count
			context = ngram[2] + u" " + ngram[1] + u" "
			num_context += 1
			if context in bigrams_dict:
				total_context_freq = bigrams_dict[context]
			else:
				#raise ValueError('Missing context: '+ context) 
				pdb.set_trace()
				#there should not be any missing values
			cond_prob = math.log(count / float(total_context_freq))
			#print cond_prob
			surprisal_total += (count * cond_prob) #this is weighted by the frequency of this context
			unweightedSurprisal +=  cond_prob #this is not
		else:
			continue	
	stop_time = time.time()
	st = None if total_freq == 0 else surprisal_total / float(total_freq)
	uwst = None if num_context == 0 else unweightedSurprisal / float(num_context)
	return (word, st, uwst, total_freq, num_context, (stop_time-start_time))


def trainSublexicalSurprisalModel(wordList, order, smoothing, smoothOrder, interpolate):	
	''' Train an n-gram language model using a list of types 

		wordList: array or list of types in the language
		order: integer representing the highest order encoded in the language model
		smoothing: Smoothing technique: 'wb' or 'kn'
		smoothOrder: list of integers, indicating which orders to smooth
		interpolate: boolean, indicating whether to use interpolation or not

	'''

	# ensure that ngram-count is on the path. shouldn't need to do this from the command line	
	#os.environ['PATH'] = os.environ['PATH']+':'+srilmPath
	
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
	lex_DF = pandas.read_csv(lexfile, encoding='utf-8')
	sublex_DF = pandas.read_csv(sublexfile, encoding='utf-8')
	wordlist_DF = pandas.read_csv(wordlist_csv, encoding='utf-8').drop_duplicates('word')	

	df = lex_DF.merge(sublex_DF, on='word').sort('frequency', ascending=False)	

	df_selected = wordlist_DF.merge(df, on='word').sort('frequency', ascending=False).dropna()
	df_selected['nchar'] = [len(x) for x in df_selected['word']]	
	
	ssCor = scipy.stats.spearmanr(-1*df_selected['mean_surprisal_weighted'], df_selected['ss'])
	ncharCor = scipy.stats.spearmanr(-1*df_selected['mean_surprisal_weighted'], df_selected['nchar'])

	print ('number of words in analysis: ' + str(len(df_selected)) + ' types')
	print ("Spearman's rho for lexical and sublexical surprisal:" + str(ssCor))
	print ("Spearman's rho for lexical and character length:" + str(ncharCor))
	
	df_selected.to_csv(outfile, index=False, encoding='utf-8')

def checkForMissingFiles(directory1, pattern1, directory2, pattern2):
	'''check which files from directory1 are not in directory2'''

	raw_files = glob.glob(os.path.join(directory1,pattern1))
	raw_filenames = [os.path.splitext(os.path.basename(x))[0] for x in raw_files]
	if len(raw_filenames) == 0:
		raise ValueError('No files matching search terms found in first directory')	
	print('Directory 1 contains '+str(len(raw_filenames)) + ' files')
	processed_files = glob.glob(os.path.join(directory2,pattern2))
	processed_filenames = [os.path.splitext(os.path.basename(x))[0] for x in processed_files]
	
	if len(raw_filenames) != len(processed_filenames):
		print('Differing number of raw and processed files')

		missing = []
		[missing.append(file) for file in raw_filenames if file not in processed_filenames]
		warnings.warn(('Missing files'))
		print(missing)		
	else:
		print('Same number of raw and processed files')	
	return (len(processed_filenames) /  (len(raw_filenames) * 1.))

def checkForBinary(command):
	test = os.popen("which "+command).read()
	if test != '':
		print(command +' found at '+test)
	else:
		raise ValueError('binary for '+command +' not found')	


def downloadCorpus(language, order, inputdir):
	raise NotImplementedError

def downloadCorpusWrapper(corpusSpecification):
	downloadCorpus(corpusSpecification['language'], corpusSpecification['order'],corpusSpecification['inputdir'])

def validateCorpus(corpusSpecification):	
	raise NotImplementedError