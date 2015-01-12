#!/bin/bash

FNAME=fixedTestIndexContent
IN=${FNAME}.txt
P=_${FNAME}_

rm -f ${P}*.txt

# remove interpunctations
tr -d [:punct:] < ${IN} > ${P}no-punct.txt

# extract terms
cat ${P}no-punct.txt | tr ' ' '\n' | tr [:upper:] [:lower:] | sort > ${P}terms.txt

# extract unique terms
uniq ${P}terms.txt ${P}terms-unique.txt

# extract unique terms (counted)
uniq -c ${P}terms.txt | sort -gr > ${P}terms-unique-count.txt

# format for java inclusion
cat ${P}terms-unique-count.txt | sed -e 's/^[ \t]*//; s/^\([0-9]*\)\s\([a-z]*\)/map.put(\"\2\", \1);/' > ${P}terms-unique-count_java.txt

# create one file per document
split -d -l 3 ${P}no-punct.txt ${P}doc

# gather data for each document
for doc in ${P}doc*; do
	# extract terms
	cat ${doc} | tr ' ' '\n' | tr [:upper:] [:lower:] | sort > ${doc}_terms.txt
	# extract unique terms
	uniq ${doc}_terms.txt ${doc}_terms-unique.txt
	# extract unique terms (counted)
	uniq -c ${doc}_terms.txt | sort -gr > ${doc}_terms-unique-count.txt
	# format for java inclusion
	cat ${doc}_terms-unique-count.txt | sed -e 's/^[ \t]*//; s/^\([0-9]*\)\s\([a-z]*\)/map.put(\"\2\", \1);/' > ${doc}_terms-unique-count_java.txt
done

# gather document frequency values
rm -f ${P}term-docfreq.txt
rm -f ${P}term-docfreq_java.txt

while read term; do
	docFreq=$(grep -irwm 1 $term ${P}doc[0-9]*_terms-unique.txt | wc -l)
  echo $term $docFreq >> ${P}term-docfreq.txt
	echo map.put\(\"$term\", $docFreq\)\; >> ${P}term-docfreq_java.txt
done < ${P}terms-unique.txt
