#!/bin/sh
# ensure environment is sane
#  $M2: maven binary path
#  $HOME_SHARED: directory where '.m2' resides in
. /data/clef/init/cruncher.sh

JVM="-Xms128m -Xmx10G -XX:MaxDirectMemorySize=10G"
MAVEN_OPTS="$MAVEN_OPTS $JVM"
export MAVEN_OPTS

MVN_CMD="$M2/mvn exec:java -Dmaven.repo.local=$HOME_SHARED/.m2 -Djava.io.tmpdir=/data/clef/tmp"
