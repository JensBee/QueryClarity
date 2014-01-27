#!/bin/sh
echo Query: $1
# memory setting
JVM_MEM="-Xms128m -Xmx2G"
# allow attaching of a profiler
#JVM_PROFILER="-XX:+UseLinuxPosixThreadCPUClocks -agentpath:profiler/lib/deployed/jdk16/linux-amd64/libprofilerinterface.so=profiler/lib,5140"
# garbage collection setting
JVM_GC="-XX:+UseConcMarkSweepGC"
# diable GC performance checks
#JVM_GC_NOCHECK="-XX:-UseGCOverheadLimit"
# create heap dumps on OOM errors
JVM_HEAP_DUMP="-XX:+HeapDumpOnOutOfMemoryError"

JVM="$JVM_MEM $JVM_PROFILER $JVM_GC $JVM_GC_NOCHECK $JVM_HEAP_DUMP"

MAVEN_OPTS="$JVM"
mvn exec:java -Dexec.mainClass="de.unihildesheim.lucene.scoring.clarity.Main" -Dexec.args="-index ./data/index -query '$1'"
