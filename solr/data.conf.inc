#!/bin/sh

# Create a known directory for temporary data
# The DTDs gets mounted there to allow SOLR importing them.

TMP_DATA=/tmp/CLEF-data

# setup temp dir
if [ -d "$TMP_DATA" ]; then
  echo "Temporary mount at ${TMP_DATA} exists. Using it."
else
  mkdir ${TMP_DATA}
  ln -s ${PWD}/source-data/dtds/ ${TMP_DATA}/dtd
fi
