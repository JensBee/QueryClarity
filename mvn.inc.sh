#!/bin/sh
# ensure environment is sane
#  $M2: maven binary path
#  $HOME_SHARED: directory where '.m2' resides in
. /data/clef/init/cruncher.sh
MVN_CMD="$M2/mvn exec:java -Dmaven.repo.local=$HOME_SHARED/.m2 -Djava.io.tmpdir=/data/clef/tmp"
