#!/bin/sh

export PATH=$PATH:/usr/bin/:/usr/sbin:/bin:/sbin:/usr/ali/bin

MYPATH=`dirname $0`
cd $MYPATH >/dev/null

find ./var/log -type f -name "zookeeper.pid.*" | sed "s/.*zookeeper.pid.\([0-9]*\)\$/\1/" | xargs -n 1 ./stop.sh

cd - >/dev/null

