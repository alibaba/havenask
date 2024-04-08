

#!/bin/bash

set -e

## compatible with macos
if ! [ -x "$(command -v realpath)" ]; then
    realpath() {
        [[ $1 = /* ]] && echo "$1" || echo "$PWD/${1#./}"
    }
fi



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

USER=`echo $USER`
USERID=`id -u`
CONTAINER_NAME=$1

SCRIPT_PATH=$(realpath "$0")
SCRIPT_DIR=$(dirname "$SCRIPT_PATH")
IMAGE_REPO="registry.cn-hangzhou.aliyuncs.com/havenask"
IMAGE_VERSION="1.2.0"
IMAGE="$IMAGE_REPO/ha3_runtime:$IMAGE_VERSION"
if [ $# -eq 2 ]; then
    IMAGE=$2
fi
REPO_DIR=$(dirname "$SCRIPT_DIR")
CONTAINER_DIR=$SCRIPT_DIR/$CONTAINER_NAME

echo "Start to run scrip"
echo "Info: Repo locatation: $REPO_DIR"

echo "Info: Container entry: $CONTAINER_DIR"

mkdir -p $CONTAINER_DIR


cat >  $CONTAINER_DIR/initContainer.sh  << EOF
##!/bin/bash
groupadd havenask
useradd -l -u $USERID -G havenask -md /home/$USER -s /bin/bash $USER
usermod -u $USERID $USER
echo -e "\n$USER ALL=(ALL) NOPASSWD:ALL\n" >> /etc/sudoers
echo "PS1='[\u@havenask \w]\\$'" > /etc/profile
echo "export TERM='xterm-256color'" >> /etc/profile
mkdir -p /mnt/ram
mount -t ramfs -o size=20g ramfs /mnt/ram
chmod a+rw /mnt/ram
EOF


chmod a+x $CONTAINER_DIR/initContainer.sh


echo "Begin pull image: ${IMAGE}"

docker pull $IMAGE

docker run --ulimit nofile=655350:655350 --privileged --cap-add SYS_ADMIN --device /dev/fuse --ulimit memlock=-1 --cpu-shares=15360 --cpu-quota=9600000 --cpu-period=100000 --memory=500000m -d --net=host --name $CONTAINER_NAME -v $HOME:$HOME -v $CONTAINER_DIR/initContainer.sh:$CONTAINER_DIR/initContainer.sh $IMAGE /sbin/init 1> /dev/null


if [ $? -ne 0 ]; then
    echo "ERROR, run container failed, please check."
    exit 3
fi

echo "Begin initialize container:"
docker exec $CONTAINER_NAME /bin/bash -c "$CONTAINER_DIR/initContainer.sh"

if [ $? -ne 0 ]; then
    echo "ERROR, initialize container failed, please check."
    exit 3
fi


sshme="docker exec --user $USER -it $CONTAINER_NAME sh -c 'cd ~ && /bin/bash' "
[ -e $CONTAINER_DIR/sshme ] && rm -rf $CONTAINER_DIR/sshme
echo $sshme > $CONTAINER_DIR/sshme
chmod +x $CONTAINER_DIR/sshme
dockerstop="docker stop $CONTAINER_NAME;docker rm -fv $CONTAINER_NAME"
stopSh=stopContainer.sh
[ -e $CONTAINER_DIR/$stopSh ] && rm -rf $CONTAINER_DIR/$stopSh
echo $dockerstop > $CONTAINER_DIR/$stopSh
chmod +x $CONTAINER_DIR/$stopSh

echo "INFO start container success"
