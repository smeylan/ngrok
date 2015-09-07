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
@click.option('--faststoragedir', type=click.Path(), help="Directory on fastest storage medium available")
@click.option('--analysisname', type=str, help="Descriptive name for the analysis")
@click.option('--corpus', type=str, help="Name of the corpus")
@click.option('--language', type=str, help="Name of the language")
def makeDirectoryStructure(faststoragedir, analysisname, corpus, language):
	'''build the directory structure for holding the intermediate files and analyses'''
	ngrok.makeDirectoryStructure(faststoragedir, analysisname, corpus, language)

#processGoogle
@cli.command() 
@click.option('--inputfile', type=click.Path(exists=True), help='The file name for input')
@click.option('--outputfile', type=click.Path(), help='The file name for output (year will be appended)')
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
@click.option('--inputdirectory', type=click.Path(exists=True), help='The directory with the input')
@click.option('--outputdirectory', type=click.Path(), help='The directory where the processed files should be output')
@click.option('--yearbin', type=int, default=0, help='How many bins of years?')
@click.option('--quiet', type=bool, default=True, help= 'Output tossed lines?')
@click.option('--n', type=int, default=3, help="Order of the ngram")
@click.option('--earliest', type=int, default=1800, help="Earliest year to include")
@click.option('--latest', type=int, default=2012, help="Latest year to include")
@click.option('--reverse', type=bool, default=False, help="Reverse the ngram?")
@click.option('--strippos', type=bool, default=True, help="Strip the part of speech information?")
@click.option('--lower', type=bool, default=False, help="Convert ngrams to lower case?")
def processGoogleDirectory(inputdirectory, outputdirectory, yearbin, quiet, n, latest, earliest, reverse, strippos, lower):	
	'''Parallelized applicatoin of processGoogle'''	
	ngrok.processGoogleDirectory(inputdirectory, outputdirectory, yearbin, quiet, n, earliest, latest, reverse, strippos, lower)


#combineFiles
@cli.command()
@click.option('--inputdirectory', type=click.Path(), help="Directory containing input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
def combineFiles(inputdirectory, outputfile):
	'''Concatenate count files; basically a wrapper for GNU cat'''	
	ngrok.combineFiles(inputdirectory, outputfile)

#sortNgramFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
def sortNgramFile(inputfile, outputfile):
	'''Sort Ngram count files alphabetically'''	
	ngrok.sortNgramFile(inputfile, outputfile)	

#collapseNgramFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
def collapseNgramFile(inputfile, outputfile):
	'''Collapse equivalent records in Google-style count files'''	
	ngrok.collapseNgramFile(inputfile, outputfile)	

#makeLanguageModel
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
@click.option('--metadata', type=str, default="none", help="Compression codec for the ZS file")
@click.option('--codec', type=str, default="{}", help="Metadata for the ZS file")
def makeLanguageModel(inputfile, outputfile, metadata, codec):
	'''Build ZS file from Google-style count files'''	
	parsed_metadata = json.loads(metadata) #string -> dictionary
	ngrok.makeLanguageModel(inputfile, outputfile, parsed_metadata, codec)

#reverseGoogleFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
def reverseGoogleFile(inputfile, outputfile):
	''' Reverse the order of all columns but the last (presumably a count) in a Google count file'''
	ngrok.reverseGoogleFile(inputfile, outputfile)

#rearrangeNgramFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
@click.option('--reverse', type=bool, default=False, help="reverse the order of the ngram?")
def rearrangeNgramFile(inputfile, outputfile, reverse):
	''' Move the count to the end and reverse, if specified, the order of the ngram for an ngram txt file produced by AutoCorpus'''
	ngrok.rearrangeNgramFile(inputfile, outputfile, reverse)

#marignalizeNgramFile
@cli.command()
@click.option('--inputfile', type=click.Path(), help="Filename of the input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
@click.option('--n', type=int, default=None, help="Order ngram to output")
@click.option('--sorttype', type=str, default=None, help="numeric or alphabetic sorting?")
def marginalizeNgramFile(inputfile, outputfile, n, sorttype):
	'''Produce lower-order aggregate counts from higher-order ngram file'''
	ngrok.marginalizeNgramFile(inputfile, outputfile, n, sorttype)

#download
@cli.command() 
def download():
	'''Requires implementation'''	
	raise NotImplementedError

#countNgrams
@cli.command() 
@click.option('--inputfile', type=click.Path(), help="Filename of the input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
@click.option('--n', type=int, default=3, help="Order of the ngram")
def countNgrams(inputfile, outputfile, n):
	'''Produce an ngram count for a text file using the ngrams command from Autocorpus'''
	ngrok.countNgrams(inputfile, outputfile, n)

#cleanTextFile
@cli.command() 
@click.option('--inputfile', type=click.Path(), help="Filename of the input files")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
@click.option('--cleaningFunction', type=str, help="Name of the cleaning function to use")
def cleanTextFile(inputfile, outputfile, cleaningFunction):
	'''Cleans a plaintext file line by line with the function specified in cleaningFunction'''
	ngrok.cleanTextFile(inputfile, outputfile, n)	

#getMeanSurprisal
@cli.command() 
@click.option('--backwards_zs', type=click.Path(), help="Filename of the backwards language model of the highest order")
@click.option('--forwards_txt', type=click.Path(), help="Filename of the forwards language model of order n-1")
@click.option('--unigram_txt', type=click.Path(), help="Filename of the unigram frequency file")
@click.option('--wordlist_csv', type=click.Path(), help="Filename of the wordlist CSV to check against; words should be in the first column")
@click.option('--cutoff', type=int, default=0, help="Discard ngrams from highest order model with frequency < n")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
def getMeanSurprisal(backwards_zs, forwards_txt, unigram_txt, wordlist_csv, cutoff, outputfile):
	'''Compute mean surprisal / information content for a list of words'''
	ngrok.getMeanSurprisal(backwards_zs, forwards_txt, unigram_txt, wordlist_csv, cutoff, outputfile)	

#getSublexicalSurprisals
@cli.command() 
@click.option('--inputfile', type=click.Path(), help="Filename of the input files. Must contain 'word' as a column name")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
@click.option('--n', type=int, help="Number of types to use in the model. Input file must be ordered for this to make sense. -1 indicates use the entire 'word' column")
@click.option('--srilmpath', type=click.Path(), help="Path for SRILM")
def getSublexicalSurprisals(inputfile, outputfile, n, srilmpath):
	'''get the probability of each word's letter sequence using the set of words in the language'''
	ngrok.getSublexicalSurprisals(inputfile, outputfile, n, srilmpath)

#analyzeSurprisalCorrelations
@cli.command() 
@click.option('--lexfile', type=click.Path(), help="Filename of for the lexical suprisal values. Must contain 'word' as a column name")
@click.option('--sublexfile', type=click.Path(), help="Filename of for the sublexical suprisal values. Must contain 'word' as a column name")
@click.option('--wordlist_csv', type=click.Path(), help="Filename of the wordlist CSV to check against; words should be in the first column")
@click.option('--outputfile', type=click.Path(), help="Filename of the output file")
def analyzeSurprisalCorrelations(lexfile, sublexfile, wordlist_csv, outputfile):
	'''get the probability of each word's letter sequence using the set of words in the language'''
	ngrok.analyzeSurprisalCorrelations(lexfile, sublexfile, wordlist_csv, outputfile)

#checkForMissingFiles
@cli.command()
@click.option('--directory1', type=click.Path(), help="Path of the first directory")
@click.option('--pattern1', type=str, help="Glob pattern for first directory")
@click.option('--directory2', type=click.Path(), help="Path of the second directory")
@click.option('--pattern2', type=str, help="Glob pattern for the second directory")
def checkForMissingFiles(directory1, pattern1, directory2, pattern2):
	'''check which files from directory1 are not in directory2'''
	ngrok.checkForMissingFiles(directory1, pattern1, directory2, pattern2)

if __name__ == '__main__':
    cli()
