#/bin/sh

set -x
export PATH=$PATH:/usr/bin/:/usr/sbin:/bin:/sbin:/usr/ali/bin
export -n LD_PRELOAD

MYPATH=`dirname $0`
cd $MYPATH >/dev/null

if [[ "x${TEST_TMPDIR}" == "x" ]]; then
    TEST_TMPDIR="."
fi

mkdir -p ${TEST_TMPDIR}/conf
sed "s#clientPort=.*#clientPort=$1#" ./conf/zoo.cfg | \
    sed "s#dataDir=.*#dataDir=${TEST_TMPDIR}/var/data/$2#" > ${TEST_TMPDIR}/conf/zoo_${2}.cfg
sed "s#log4j.appender.ROLLINGFILE.File=.*#log4j.appender.ROLLINGFILE.File=${TEST_TMPDIR}/var/log/zookeeper_${2}.log#" ./conf/log4j.properties > ${TEST_TMPDIR}/conf/log4j.properties
mkdir -p ${TEST_TMPDIR}/var/data/$2/
mkdir -p ${TEST_TMPDIR}/var/log/

for i in `seq 1 100`; do
  V=`echo "ruok" | nc -i 2 127.0.0.1 $1`
  if [ x"$V" != x"imok" ]; then
      break
  fi
  usleep 400000
done

if [ $i == 100 ]; then
    echo "service already exists"
    exit 1
fi

nohup java -Dzookeeper.log.dir=. -Dzookeeper.root.logger=INFO,CONSOLE -cp ./zookeeper-3.5.4-beta.jar:./lib/slf4j-api-1.7.25.jar:./lib/slf4j-log4j12-1.7.25.jar:./lib/log4j-1.2.17.jar:./lib/jline-2.11.jar:${TEST_TMPDIR}/conf: -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false org.apache.zookeeper.server.quorum.QuorumPeerMain ${TEST_TMPDIR}/conf/zoo_${2}.cfg </dev/null >${TEST_TMPDIR}/var/log/zk.log 2>&1 &

for i in `seq 1 100`; do
  V=`echo "ruok" | nc -i 2 127.0.0.1 $1`
  if [ x"$V" == x"imok" ]; then
      break
  fi
  if [ x"$V" == x"1" ]; then
      break
  fi
  usleep 400000
done

if [ $i == 100 ]; then
    echo "can't start service"
    exit 1
fi

jobs -p >${TEST_TMPDIR}/var/log/zookeeper.pid.$2
# tail -n 30 ${TEST_TMPDIR}/var/log/zookeeper.log
cd - >/dev/null
