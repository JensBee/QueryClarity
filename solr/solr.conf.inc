#!/bin/sh

# SOLR configuration
SOLR_PORT=8983
SOLR_CONTROL_PORT=8079
SOLR_PATH=solr-4.7.2
SOLR_HOME='./clef/'
SOLR_CORE='db4'

# java setup for SOLR
XMX=3072m
XMS=512m
# JAVA=/opt/java7/bin/java
JAVA=/usr/bin/java

# SOLR URLs
URL_BASE="127.0.0.1:${SOLR_PORT}/solr/${SOLR_CORE}"
URL_UPDATE="${URL_BASE}/clef/import?optimize=false&clean=false&commit=false&verbose=false&entity=clef-IP-XSL&command=full-import&path="
URL_UPDATE_COMMIT="${URL_BASE}/clef/import?commit=true"
URL_IMPORTED="${URL_BASE}/select?q=*:*&fl=id&wt=csv" # need to attach &start=.. and &rows=..
URL_STATUS="${URL_BASE}/clef/import"
URL_BACKUP="${URL_BASE}/replication?command=backup"
