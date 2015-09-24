# ngrok must be on the path

# initialize the directory structure
ngrok makedirectorystructure --faststoragedir /shared_ssd/ss/ --analysisname sep2 --corpus GoogleBooks2012 --language Eng

#clean the google trigrams
ngrok processgoogledirectory --inputdir /shared_hd/corpora/GoogleBooks2012/eng/3/ --outputdir /shared_hd/corpora/GoogleBooks2012/eng/3-processed/ --yearbin 0 --quiet True --n 3 --earliest 1800 --latest 2012 --reverse True --strippos True --lower True

# check if there are any files from the input directory which are not in the output firectoy
ngrok checkformissingfiles --directory1 /shared_hd/corpora/GoogleBooks2012/eng/2 --pattern1 *.gz  --directory2 /shared_hd/corpora/GoogleBooks2012/eng/2-processed  --pattern2 *.output.0

# combine the processed backwards trigram files
ngrok combinefiles --inputdirectory ~/GB/Eng/backwards --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards.txt


# sort the processed backwards trigram file
ngrok sortngramfile --inputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards-sorted.txt 


# merge records that are now proximal because of the sorting
ngrok collapsengramfile --inputfile  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards-sorted.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards-collapsed.txt


# build the ZS language model for the backwards trigrams
ngrok makelanguagemodel --inputfile  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards-collapsed.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards.zs --metadata '{"corpus":"Google Books 2012","language":"english","order":"3", "direction":"backwards"}' --codec "none"

#sanity check on the language model
zs dump /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards.zs --prefix='them\tnear\t'


# to produce bigram counts, need to reverse first, then marginalize
# reverse backards trigram language model
ngrok reversegooglefile --inputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards-collapsed.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-forwards-cleaned.txt

# sort the forwards trigram language model
ngrok sortngramfile --inputfile  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-forwards-cleaned.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-forwards-sorted.txt


# marginalize the forwards trigram langauge model to produce the forwards bigram model
ngrok marginalizengramfile --inputfile  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-forwards-sorted.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards-sorted.txt --n 2 --sorttype alphabetic

# collapsing is not necessary, so we can just copy the sorted to the collapsed filename
cp /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards-sorted.txt /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards-collapsed.txt

# build the forwards bigram language model
ngrok makelanguagemodel --inputfile  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards-collapsed.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards.zs --metadata '{"corpus":"Google Books 2012","language":"english","order":"2", "direction":"forwards"}' --codec "none"

# marginalize the forwards bigram language model to prodcuce the forwards unigram langauge model
ngrok marginalizengramfile --inputfile  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards-collapsed.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/1gram-forwards-collapsed.txt --n 1 --sorttype alphabetic

# build the forwards unigram language model
ngrok makelanguagemodel --inputfile  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/1gram-forwards-collapsed.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/1gram-forwards.zs --metadata '{"corpus":"Google Books 2012","language":"english","order":"1", "direction":"forwards"}' --codec "none"

# build the unigram count list. Unlike the forwards unigram language model which is sorted alphabetically, the unigram count list is sorted descending by frequency
ngrok marginalizengramfile --inputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards-collapsed.txt --n 1 --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/unigram_list.txt --sorttype numeric

# add word to the unigram list column name
sed -i '1s/^/count\\tword\\n/' test.txt /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/unigram_list.txt

# get the sublexical surprisal estimates
ngrok getsublexicalsurprisals --inputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/unigram_list.txt --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/01_sublexicalSurprisal/200000_sublex.csv --n 200000 --srilmpath /usr/share/srilm/bin/i686-m64/

# get the mean sublexical surprisal estimates -- Switchboard wordlist
ngrok getmeansurprisal --backwards_zs /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards.zs --forwards_txt /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards-collapsed.txt --unigram_txt  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/unigram_list.txt --wordlist_csv /shared_hd/corpora/Switchboard/switchboardWordList.csv --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/meanSurpisal_25k_switchboard.csv

# analyze the correlations from the Switchboard wordlist
ngrok analyzesurprisalcorrelations --lexfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/meanSurpisal_25k_switchboard.csv --sublexfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/01_sublexicalSurprisal/200000_sublex.csv --wordlist_csv /shared_hd/corpora/Switchboard/switchboardWordList.csv --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/02_correlations/switchboard_sublex200000.csv

# get the mean sublexical surprisal estimates -- OPUS wordlist
ngrok getmeansurprisal --backwards_zs /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/3gram-backwards.zs --forwards_txt /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/2gram-forwards-collapsed.txt --unigram_txt  /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/unigram_list.txt --wordlist_csv /shared_hd/corpora/OPUS/en_opus_wordlist.csv --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/meanSurpisal_25k_opus.csv

# analyze the correlations from the OPUS wordlist
ngrok analyzesurprisalcorrelations --lexfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/00_lexicalSurprisal/meanSurpisal_25k_opus.csv --sublexfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/01_sublexicalSurprisal/200000_sublex.csv --wordlist_csv /shared_hd/corpora/OPUS/en_opus_wordlist.csv --outputfile /shared_ssd/ss/sep2/GoogleBooks2012/Eng/02_correlations/25k_opus_sublex200000.csv