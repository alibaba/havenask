#/bin/sh

export PATH=$PATH:/usr/bin/:/usr/sbin:/bin:/sbin:/usr/ali/bin
export -n LD_PRELOAD

MYPATH=`dirname $0`
cd $MYPATH >/dev/null

if [[ "x${TEST_TMPDIR}" == "x" ]]; then
    TEST_TMPDIR="."
fi

PID=`cat ${TEST_TMPDIR}/var/log/zookeeper.pid.$1 2>/dev/null`
if [ x"$PID" == x"" ]; then
    exit
fi
for i in `seq 1 10`;
  do
  V=`ps -p $PID | grep java | wc -l`
  if [ x"$V" == x"1" ]; then
      kill $PID >/dev/null 2>&1
  else
      break
  fi
  usleep 10
done
rm ${TEST_TMPDIR}/var/log/zookeeper.pid.$1
rm -rf ${TEST_TMPDIR}/var/data/$1
rm ${TEST_TMPDIR}/conf/zoo_${1}.cfg
cd - >/dev/null

