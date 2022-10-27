#!/bin/bash
#DIR="$(dirname "${BASH_SOURCE[0]}")"
#DIR="$(realpath "${DIR}")"

set -e

if [ "$1" == "-h" -o "$1" == "--help" ]; then
    echo "Usage: $0 DOCKER_NAME [IMAGE]"
    echo "       then excute './DOCKER_NAME/sshme' to enter to container."
    exit 0
fi
if [ $# -lt 1 ]; then
    echo "Usage: $0 DOCKER_NAME [IMAGE]"
    echo "       then excute './DOCKER_NAME/sshme' to enter to container."
    exit 2
fi


IMAGE="havenask/ha3_dev:0.1"
if [ $# -eq 2 ]; then
    IMAGE=$2
fi

USER=`echo $USER`
NAME=$1
CUR_DIR=`pwd`/$NAME
HOME=`eval echo ~$USER`
mkdir -p $NAME
rm -rf $CUR_DIR/.group
rm -rf $CUR_DIR/.passwd
rm -rf $CUR_DIR/.ssh

name=$NAME
if [ -z $name ];then
    name="${USER}_dev"
fi

docker run --ulimit nofile=655350:655350 --cap-add SYS_ADMIN --device /dev/fuse --ulimit memlock=-1 --cpu-shares=15360 --cpu-quota=9600000 --cpu-period=100000 --memory=500000m -d --net=host --name $name -v $HOME:$HOME -v $HOME:/home/$USER -v /etc/hosts:/etc/hosts:ro -v /var/run/docker.sock:/tmp/docker.sock $IMAGE /sbin/init 1> /dev/null

if [ $? -ne 0 ]; then
    echo "ERROR, run container failed, please check."
    exit 3
fi

sshme="docker exec -it $NAME bash"
[ -e $CUR_DIR/sshme ] && rm -rf $CUR_DIR/sshme
echo $sshme > $CUR_DIR/sshme
chmod +x $CUR_DIR/sshme
dockerstop="docker stop $name;docker rm -fv $name"
stopSh=stopContainer.sh
[ -e $CUR_DIR/$stopSh ] && rm -rf $CUR_DIR/$stopSh
echo $dockerstop > $CUR_DIR/$stopSh
chmod +x $CUR_DIR/$stopSh

echo "INFO start container success"
