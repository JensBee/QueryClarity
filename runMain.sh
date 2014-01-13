#!/bin/sh
echo Query: $1
mvn exec:java -Dexec.mainClass="de.unihildesheim.lucene.queryclarity.Main" -Dexec.args="-index /media/Brain2/CLEF/solr/solr-4.6.0/example/clef/db/data/index -query '$1'"