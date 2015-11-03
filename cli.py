#!/usr/bin/python
# -*- coding: utf-8 -*-

#this file provides CLI hooks for the functions in the ngrok library. For running many corpora, look at main.py
import ngrok, click, json

CONTEXT_SETTINGS = dict(help_option_names=['-h', '--help'])

@click.group(CONTEXT_SETTINGS)
def cli():
    pass

#makeDirectoryStructure
@click.command()
@click.option('--faststoragedir', type=click.Path(), help="Directory on fastest storage medium available", required=True)
@click.option('--slowstoragedir', type=click.Path(), help="Directory on the larges storage medium", required=True)
@click.option('--analysisname', type=str, help="Descriptive name for the analysis", required=True)
@click.option('--corpus', type=str, help="Name of the corpus", required=True)
@click.option('--language', type=str, help="Name of the language", required=True)
@click.option('--n', type=int, help="highest order of ngram model to make", required=True)
def makeDirectoryStructure(faststoragedir, slowstoragedir, analysisname, corpus, language, n):
	'''build the directory structure for holding the intermediate files and analyses'''
	ngrok.makeDirectoryStructure(faststoragedir, analysisname, corpus, language, n)

#cleanGoogle
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input', required=True)
@click.option('--outputfile', type=click.Path(), help='The file name for output (year will be appended)', required=True)
@click.option('--collapseyears', type=bool, help='Collapse the counts over years?')
@click.option('--filetype', type=str, help='Extension of the file that is to be cleaned')
@click.option('--order', type=int, help="order of ngrams to download", required=True)
def cleanGoogle(inputfile, outputfile, collapseyears, filetype, order):	
	'''Strip punctuation from google-formatted ngram files and retain only lines that are words alone (e.g. no POS tags)'''	
	ngrok.cleanGoogle(inputfile, outputfile, collapseyears, filetype, order)

#collapseNgrams
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input', required=True)
@click.option('--outputfile', type=click.Path(), help='The file name for output (year will be appended)', required=True)
def collapseNgrams(inputfile, outputfile):	
	'''Collapse dates from Google-formatted ngram files'''	
	ngrok.collapseNgrams(inputfile, outputfile)	

#processGoogle
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input', required=True)
@click.option('--outputfile', type=click.Path(), help='The file name for output (year will be appended)', required=True)
@click.option('--yearbin', type=int, default=0, help='How many bins of years?')
@click.option('--quiet', type=bool, default=True, help= 'Output tossed lines?')
@click.option('--n', type=int, default=3, help="Order of the ngram")
@click.option('--latest', type=int, default=2012, help="Latest year to include")
@click.option('--earliest', type=int, default=1800, help="Earliest year to include")
@click.option('--reverse', type=bool, default=False, help="Reverse the ngram?")
@click.option('--strippos', type=bool, default=True, help="Strip the part of speech information?")
@click.option('--lower', type=bool, default=False, help="Convert ngrams to lower case?")
def processGoogle(inputfile, outputfile, yearbin, quiet, n, latest, earliest, reverse, strippos, lower):	
	'''Clean and shuffle columns of Google-style count file(s), separating into year bins if desired'''	
	ngrok.processGoogle(inputfile, outputfile, yearbin, quiet, n, earliest, latest, reverse, strippos, lower)

#processGoogleDirectory -- parallelized version of processGoogle
@cli.command() 
@click.option('--inputdir', type=click.Path(exists=True), help='The directory with the input')
@click.option('--outputdir', type=click.Path(), help='The directory where the processed files should be output')
@click.option('--yearbin', type=int, default=0, help='How many bins of years?')
@click.option('--quiet', type=bool, default=True, help= 'Output tossed lines?')
@click.option('--n', type=int, default=3, help="Order of the ngram")
@click.option('--earliest', type=int, default=1800, help="Earliest year to include")
@click.option('--latest', type=int, default=2012, help="Latest year to include")
@click.option('--reverse', type=bool, default=False, help="Reverse the ngram?")
@click.option('--strippos', type=bool, default=True, help="Strip the part of speech information?")
@click.option('--lower', type=bool, default=False, help="Convert ngrams to lower case?")
def processGoogleDirectory(inputdir, outputdir, yearbin, quiet, n, latest, earliest, reverse, strippos, lower):	
	'''Parallelized applicatoin of processGoogle'''	
	ngrok.processGoogleDirectory(inputdir, outputdir, yearbin, quiet, n, earliest, latest, reverse, strippos, lower)


#cleanGoogleDirectory -- parallelized version of cleanGoogle
@cli.command() 
@click.option('--inputdir', type=click.Path(exists=True), help='The directory with the input', required=True)
@click.option('--outputdir', type=click.Path(), help='The directory where the processed files should be output', required=True)
@click.option('--collapseyears', type=bool, help='Collapse the counts over years?', required=True)
@click.option('--order', type=int, help="order of ngrams to download", required=True)
def cleanGoogleDirectory(inputdir, outputdir, collapseyears):	
	'''Parallelized application of processGoogle'''	
	ngrok.cleanGoogleDirectory(inputdir, outputdir, collapseyears)

#combineFiles
@cli.command()
@click.option('--inputdir', type=click.Path(), help="Directory containing input files", required=True)
@click.option('--pattern', type=str, help="String to match in deciding which files to concatenate", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
def combineFiles(inputdir, pattern, outputfile):
	'''Concatenate count files; basically a wrapper for GNU cat'''	
	ngrok.combineFiles(inputdir, outputfile)

#sortNgramFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
def sortNgramFile(inputfile, outputfile):
	'''Sort Ngram count files alphabetically'''	
	ngrok.sortNgramFile(inputfile, outputfile)	

#makeLanguageModel
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
@click.option('--metadata', type=str, default="none", help="Compression codec for the ZS file", required=True)
@click.option('--codec', type=str, default="{}", help="Metadata for the ZS file")
def makeLanguageModel(inputfile, outputfile, metadata, codec):
	'''Build ZS file from Google-style count files'''	
	parsed_metadata = json.loads(metadata) #string -> dictionary
	ngrok.makeLanguageModel(inputfile, outputfile, parsed_metadata, codec)

#reverseGoogleFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
def reverseGoogleFile(inputfile, outputfile):
	''' Reverse the order of all columns but the last (presumably a count) in a Google count file'''
	ngrok.reverseGoogleFile(inputfile, outputfile)

#deriveFromHigherOrderModel
@cli.command()
@click.option('--intermediatefiledir', type=click.Path(), help="Directory with sorted ngram models")
@click.option('--n', type=int, help="Order of the desired model")
@click.option('--direction', type=str, help="Direction of the desired model. Specify either forwards or backwards")
def deriveFromHigherOrderModel(intermediatefiledir, n, direction):
	'''Search for a pre-computed model from which the desired counts can be derived through some combination of reversing or marginalization. Specify the desired order and direction and the function looks for appropriate files to use in order to create it. This is faster than cleaning the data and deriving new counts.'''
	ngrok.deriveFromHigherOrderModel(intermediatefiledir, n, direction)

#rearrangeNgramFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
@click.option('--reverse', type=bool, default=False, help="reverse the order of the ngram?", required=True)
def rearrangeNgramFile(inputfile, outputfile, reverse):
	''' Move the count to the end and reverse, if specified, the order of the ngram for an ngram txt file produced by AutoCorpus'''
	ngrok.rearrangeNgramFile(inputfile, outputfile, reverse)

#marignalizeNgramFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
@click.option('--n', type=int, default=None, help="Order ngram to output", required=True)
@click.option('--sorttype', type=str, default=None, help="numeric or alphabetic sorting?", required=True)
def marginalizeNgramFile(inputfile, outputfile, n, sorttype):
	'''Produce lower-order aggregate counts from higher-order ngram file'''
	ngrok.marginalizeNgramFile(inputfile, outputfile, n, sorttype)

#countNgrams
@cli.command() 
@click.option('--inputfile', type=click.Path(), help="Filename of the input files", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
@click.option('--n', type=int, default=3, help="Order of the ngram", required=True)
def countNgrams(inputfile, outputfile, n):
	'''Produce an ngram count for a text file using the ngrams command from Autocorpus'''
	ngrok.countNgrams(inputfile, outputfile, n)

#cleanTextFile
@cli.command() 
@click.option('--inputfile', type=click.Path(), help="Filename of the input files", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
@click.option('--cleaningFunction', type=str, help="Name of the cleaning function to use", required=True)
def cleanTextFile(inputfile, outputfile, cleaningFunction):
	'''Cleans a plaintext file line by line with the function specified in cleaningFunction'''
	ngrok.cleanTextFile(inputfile, outputfile, n)	

#getMeanSurprisal
@cli.command() 
@click.option('--backwards_zs', type=click.Path(), help="Filename of the backwards language model of the highest order", required=True)
@click.option('--forwards_txt', type=click.Path(), help="Filename of the forwards language model of order n-1", required=True)
@click.option('--unigram_txt', type=click.Path(), help="Filename of the unigram frequency file", required=True)
@click.option('--wordlist_csv', type=click.Path(), help="Filename of the wordlist CSV to check against; words should be in the first column", required=True)
@click.option('--cutoff', type=int, default=0, help="Discard ngrams from highest order model with frequency < n", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
def getMeanSurprisal(backwards_zs, forwards_txt, unigram_txt, wordlist_csv, cutoff, outputfile):
	'''Compute mean surprisal / information content for a list of words'''
	ngrok.getMeanSurprisal(backwards_zs, forwards_txt, unigram_txt, wordlist_csv, cutoff, outputfile)	

#addSublexicalSurprisals
@cli.command() 
@click.option('--inputfile', type=click.Path(), help="Filename of the input files. Must contain 'word' as a column name", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
@click.option('--column', type=str, help="name of the column to build the sublexical surprisal model from", required=True)
@click.option('--n', type=int, help="Number of types to use in the model. Input file must be ordered for this to make sense. -1 indicates use the entire 'word' column")
@click.option('--language', type=str, help="2-letter language code", required=True)
def addSublexicalSurprisals(inputfile, outputfile, column, n, language):
	'''get the probability of each word's letter sequence using the set of words in the language'''
	ngrok.addSublexicalSurprisals(inputfile, outputfile, column, n, language)

#analyzeSurprisalCorrelations
@cli.command() 
@click.option('--lexfile', type=click.Path(), help="Filename of for the lexical suprisal values. Must contain 'word' as a column name", required=True)
@click.option('--sublexfile', type=click.Path(), help="Filename of for the sublexical suprisal values. Must contain 'word' as a column name", required=True)
@click.option('--wordlist_csv', type=click.Path(), help="Filename of the wordlist CSV to check against; words should be in the first column", required=True)
@click.option('--outputfile', type=click.Path(), help="Filename of the output file", required=True)
def analyzeSurprisalCorrelations(lexfile, sublexfile, wordlist_csv, outputfile):
	'''get the probability of each word's letter sequence using the set of words in the language'''
	ngrok.analyzeSurprisalCorrelations(lexfile, sublexfile, wordlist_csv, outputfile)

#checkForMissingFiles
@cli.command()
@click.option('--directory1', type=click.Path(), help="Path of the first directory", required=True)
@click.option('--pattern1', type=str, help="Glob pattern for first directory", required=True)
@click.option('--directory2', type=click.Path(), help="Path of the second directory", required=True)
@click.option('--pattern2', type=str, help="Glob pattern for the second directory", required=True)
def checkForMissingFiles(directory1, pattern1, directory2, pattern2):
	'''check which files from directory1 are not in directory2'''
	ngrok.checkForMissingFiles(directory1, pattern1, directory2, pattern2)

#downloadCorpus
@cli.command()
@click.option('--language', type=str, help="Name of the language to download", required=True)
@click.option('--order', type=int, help="order of ngrams to download", required=True)
@click.option('--inputdir', type=click.Path(), help="path of where to write the downloaded ngram files")
def downloadCorpus(language, order, inputdir):
	ngrok.downloadCorpus(language, order, inputdir)

#cleanUnigramCountFile
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input', required=True)
@click.option('--outputfile', type=click.Path(), help='The file name for output (year will be appended)', required=True)
@click.option('--n', type=int, help='The top n words to retain', required=True)
def cleanUnigramCountFile(inputfile, outputfile, n):
	'''Filter the unigram count file, and reduce the number of items in it.'''
	ngrok.cleanUnigramCountFile(inputfile, outputfile, n)

#fixPunctuation
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input', required=True)
@click.option('--outputfile', type=click.Path(), help='The file name for output', required=True)
@click.option('--order', type=int, help="order of ngrams to download", required=True)
def fixPunctuation(inputfile, outputfile, order):	
	ngrok.fixPunctuation(inputfile, outputfile, order)

#letterize
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input', required=True)
@click.option('--outputfile', type=click.Path(), help='The file name for output', required=True)
@click.option('--splitwords', type=bool, help='Utterance or word-based model. If utterance, set to False', required=True)
@click.option('--espeak_lang', type=str, help='Language for espeak. Specify none to return letters', required=True)
@click.option('--phonebreak', type=str, help='string to place between letters', required=True)
@click.option('--par', type=bool, help='Parallelize the function?', required=True)
def letterize(inputfile, outputfile, splitwords, espeak_lang, phonebreak, par):	
	ngrok.letterize(inputfile, outputfile, splitwords, espeak_lang, phonebreak, par)

#filterByWordList
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input', required=True)
@click.option('--outputfile', type=click.Path(), help='The file name for output', required=True)
@click.option('--loweronly', type=bool, help='should upper case items be excluded from the count? (if so, set to True)', required=True)
@click.option('--vocabfile', type=click.Path(), help='The file name for the wordlist to use. Must have a "word" column', required=True)
@click.option('--n', type=int, help='The top n words to retain', required=True)
@click.option('--par', type=bool, help='Parallelize the function?', required=True)
def filterByWordList(inputfile, outputfile, loweronly, vocabfile, n, par):	
	ngrok.filterByWordList(inputfile, outputfile, loweronly, vocabfile, n, par)


#splitfile
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input', required=True)
@click.option('--n', type=int, help='Number of equal-sized chunks', required=True)
def splitfile(inputfile, n):	
	ngrok.splitfile(inputfile, n)


if __name__ == '__main__':
    cli()