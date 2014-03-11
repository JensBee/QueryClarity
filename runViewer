#!/bin/sh

# memory setting
JVM_MEM="-Xms128m -Xmx2G"
JVM_DEFAULT="-server" # -d64

JVM="$JVM_DEFAULT $JVM_MEM"
MAVEN_OPTS="$MAVEN_OPTS $JVM"

mvn exec:java -Dexec.mainClass="de.unihildesheim.lucene.index.CachedIndexViewer" -Dexec.args="-storageId clef $*"
