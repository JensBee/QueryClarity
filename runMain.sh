#!/bin/sh

# LUCENE 5212 5158
# JDK 8024830

echo Query: $1
# memory setting
JVM_MEM="-Xms128m -Xmx2G"
# allow attaching of a profiler
#JVM_PROFILER="-XX:+UseLinuxPosixThreadCPUClocks -agentpath:profiler/lib/deployed/jdk16/linux-amd64/libprofilerinterface.so=profiler/lib,5140"

# garbage collection setting
JVM_GC="-XX:+UseConcMarkSweepGC -XX:+CMSClassUnloadingEnabled"
#JVM_GC="-XX:+UnlockExperimentalVMOptions -XX:+UseG1GC" # may cause errors: LUCENE-5168
#JVM_GC="-XX:+UseParallelGC" # the default GC

# diable GC performance checks
#JVM_GC_NOCHECK="-XX:-UseGCOverheadLimit"
# create heap dumps on OOM errors
#JVM_HEAP_DUMP="-XX:+HeapDumpOnOutOfMemoryError -XX:-UseCompressedOops"

# switch java versions
#export JAVA_HOME=/opt/java7

# extra params
#JVM_EXTRA="-XX:-UseLoopPredicate" # may prevent from SIGSEGVs - see JDK-8024830

JVM_DEFAULT="-server" # -d64

JVM="$JVM_DEFAULT $JVM_MEM $JVM_PROFILER $JVM_GC $JVM_GC_NOCHECK $JVM_HEAP_DUMP"

MAVEN_OPTS="$MAVEN_OPTS $JVM"

echo
echo --JAVA-INFO--
echo --JAVA_HOME=$JAVA_HOME
echo --MAVEN_OPTS=$MAVEN_OPTS
echo --VERSION:
java -version
echo --DEFAULT FLAGS/GC:
java $JVM -XX:+PrintCommandLineFlags -XX:+PrintGCDetails
echo --JAVA-INFO--
echo
mvn exec:java -Dexec.mainClass="de.unihildesheim.lucene.scoring.clarity.Main" -Dexec.args="-index ./data/index -query '$1'"
