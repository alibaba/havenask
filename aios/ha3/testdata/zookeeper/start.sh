#/bin/sh

export PATH=$PATH:/usr/bin/:/usr/sbin:/bin:/sbin:/usr/ali/bin
export -n LD_PRELOAD

MYPATH=`dirname $0`
cd $MYPATH >/dev/null

sed "s/clientPort=.*/clientPort=$1/" ./conf/zoo.cfg > ./conf/zoo_${2}.cfg
sed -i "s/dataDir=.*/dataDir=.\/var\/data\/$2/" ./conf/zoo_${2}.cfg
mkdir -p ./var/data/$2/

for i in `seq 1 100`;
  do
#  echo "" | nc 127.0.0.1 $1
  V=`./cli_st 127.0.0.1:$1 cmd:'ls /' 2>&1 | grep CONNECTED_STATE | wc -l`
  if [ x"$V" == x"0" ]; then
      break
  fi
  usleep 400000
done

if [ $i == 100 ]; then
    echo "can't start service"
fi

nohup java -Dzookeeper.log.dir=. -Dzookeeper.root.logger=INFO,CONSOLE -cp ./classes:./lib/log4j-1.2.15.jar:./lib/jline-0.9.94.jar:./zookeeper-3.3.2.jar:./lib/log4j-1.2.15.jar:./lib/jline-0.9.94.jar:./lib/ivy-2.1.0.jar:./conf: -Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.local.only=false org.apache.zookeeper.server.quorum.QuorumPeerMain ./conf/zoo_${2}.cfg </dev/null >./zk.log 2>&1 &

for i in `seq 1 100`;
  do
#  echo "" | nc 127.0.0.1 $1
  V=`./cli_st 127.0.0.1:$1 cmd:'ls /' 2>&1 | grep CONNECTED_STATE | wc -l`
  if [ x"$V" == x"1" ]; then
      break
  fi
  usleep 400000
done

if [ $i == 100 ]; then
    echo "can't start service"
fi

jobs -p >./var/log/zookeeper.pid.$2
# tail -n 30 ./var/log/zookeeper.log
cd - >/dev/null

