#!/bin/sh
echo Query: $1
export MAVEN_OPTS=-Xmx1G
mvn exec:java -Dexec.mainClass="de.unihildesheim.lucene.scoring.clarity.Main" -Dexec.args="-index ./data/index -query '$1'"
