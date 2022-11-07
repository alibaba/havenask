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


IMAGE="havenask/ha3_runtime:0.1"
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

prepare_account()
{
    local userline=`cat /etc/passwd | awk -F: '{if ($1=="'$USER'") {print}}'`
    echo $userline > $CUR_DIR/.passwd
    local gid=`id -g $USER`
    local groupline=`cat /etc/group | awk -F: '{if ($3=="'$gid'"){print}}'`
    export GROUP=`echo $groupline | awk -F: '{print $1}'`
    echo $groupline > $CUR_DIR/.group
    chown $USER:$GROUP $CUR_DIR/.passwd
    chown $USER:$GROUP $CUR_DIR/.group
}

prepare_ssh()
{
    if  ! `mkdir -p $CUR_DIR/.ssh ` || ! `chmod 700 $CUR_DIR/.ssh` ; then
	echo "ERROR: mkdir $CUR_DIR/.ssh failed."
	exit 3
    fi

    if [ -f $HOME/.ssh/authorized_keys ]; then
	cp -f $HOME/.ssh/authorized_keys $CUR_DIR/.ssh/authorized_keys
    fi
    if [ ! -f $HOME/.ssh/id_rsa.pub ]; then
	echo "error, public key $HOME/.ssh/id_rsa.pub not exist, auto login can't be enabled, use ssh-keygen to generate"
        exit 3
    fi
    if [ -f $HOME/.ssh/id_rsa.pub ]; then
	if ! `cat $HOME/.ssh/id_rsa.pub >> $CUR_DIR/.ssh/authorized_keys` || ! `chmod 600 $CUR_DIR/.ssh/authorized_keys` ; then
	    echo "error, copy home rsa key to $CUR_DIR/.ssh/authorized_keys failed"
	fi
        if ! `cp $HOME/.ssh/id_rsa $CUR_DIR/.ssh/` ; then
            echo "error, cp private key failed, git clone may failed"
        fi
    fi
    if [ -f $HOME/.ssh/id_dsa.pub ]; then
	if ! `cat $HOME/.ssh/id_dsa.pub >> $CUR_DIR/.ssh/authorized_keys`; then
	    echo "error, copy home dsa key to $CUR_DIR/.ssh/authorized_keys failed"
	fi
    fi
    chown -R $USER:$GROUP $CUR_DIR/.ssh
}

cat > $CUR_DIR/after_start.sh <<'EOF'
#!/bin/bash

cat <<EOF9 >>/etc/profile
# make `docker ps -a` available in docker
sudo rm -rf /var/run/docker.sock
sudo ln -s /tmp/docker.sock /var/run/docker.sock
sudo setfacl -m user:$USER:rw /var/run/docker.sock
# export TERM color
export TERM="xterm-256color"
EOF9


sed -i "s%\$SSHD \$OPTIONS%/sbin/start_sshd \$OPTIONS%" /etc/init.d/sshd
sed -i "s%\$SSHD \$OPTIONS%/sbin/start_sshd \$OPTIONS%" /etc/rc.d/rc3.d/S*sshd
sed -i "s%/usr/sbin/sshd -D \$OPTIONS%/sbin/start_sshd \$OPTIONS%" /usr/lib/systemd/system/sshd.service

cat <<EOF2 >/sbin/start_sshd
#!/bin/bash
PYTHON=/usr/bin/python
SSHD=/usr/sbin/sshd
for i in {1..5}; do
    \$SSHD -p $PORT \$*
    RETVAL=\$?
    if [ \$RETVAL -eq 0 ] ; then
        echo "$PORT" > /etc/sshd_port
        break
    fi
done
EOF2

chmod a+x /sbin/start_sshd

if [ -f /tmp/.passwd ]; then
    hippouser=`cat /tmp/.passwd | awk -F: '{print $1}'`
    cat /etc/passwd | awk -F: '{print $1}' | grep -w -q $hippouser
    if [ $? -eq 0 ]; then
	sed -i "/$hippouser:x:/d" /etc/passwd
    fi
    echo `cat /tmp/.passwd` >> /etc/passwd

    cat /etc/passwd | awk -F: '{print $1}' | grep -w -q $hippouser
    if [ $? -eq 0 ]; then
	sed -i "/$hippouser:/d" /etc/shadow
    fi
    echo "$hippouser:x:17000:0:99999:7:::" >> /etc/shadow
fi

if [ -f /tmp/.group ]; then
    hippogroup=`cat /tmp/.group | awk -F: '{print $1}'`
    cat /etc/group | awk -F: '{print $1}' | grep -w -q $hippogroup
    if [ $? -eq 0 ]; then
	sed -i "/$hippogroup:x:/d" /etc/group
    fi
    echo `cat /tmp/.group` >> /etc/group
fi

echo -e "\n$USER ALL=(ALL) NOPASSWD:ALL\n" >> /etc/sudoers

# change home dir ownership to make sshd work
if [ -d /home/$USER ]; then
    chown $USER:$GROUP /home/$USER
    chmod 755 /home/$USER
fi

yum clean all

EOF

prepare_account
prepare_ssh

SSHPORT=`python -c 'import socket; s=socket.socket(); s.bind(("", 0)); print(s.getsockname()[1]); s.close()'`
sed -i "s/\$PORT/$SSHPORT/g" $CUR_DIR/after_start.sh
sed -i "s/\$USER/$USER/g" $CUR_DIR/after_start.sh
sed -i "s/\$GROUP/$GROUP/g" $CUR_DIR/after_start.sh

chmod a+x $CUR_DIR/after_start.sh
chown  $USER:$GROUP $CUR_DIR/after_start.sh

name=$NAME
if [ -z $name ];then
    name="${USER}_dev"
fi

#if [ ! -d "/mnt/ram" ]; then
#    sudo mkdir -p "/mnt/ram"
#    sudo mount -t ramfs -o size=20g ramfs /mnt/ram
#    sudo chmod a+rw /mnt/ram
#fi

#docker run --ulimit nofile=655350:655350 --cap-add SYS_ADMIN --device /dev/fuse --ulimit memlock=-1 --cpu-shares=15360 --cpu-quota=9600000 --cpu-period=100000 --memory=500000m -d --net=host --name $name -v $HOME:$HOME -v $CUR_DIR/.ssh/:$HOME/.ssh -v $CUR_DIR/after_start.sh:/tmp/after_start.sh  -v $CUR_DIR/.passwd:/tmp/.passwd -v $CUR_DIR/.group:/tmp/.group -v /etc/sysconfig/network:/etc/sysconfig/network:ro -v /etc/sysconfig/network-scripts:/etc/sysconfig/network-scripts:ro -v /etc/hosts:/etc/hosts:ro -v /etc/yum.repos.d/:/etc/yum.repos.d.mount/:ro -v /ssd:/ssd -v /home/hippo/hippo_7u/install_root/hippo_hadoop_root/usr/local:/home/hippo/hippo_7u/install_root/hippo_hadoop_root/usr/local -v /mnt/ram:/mnt/ram -v /var/run/docker.sock:/tmp/docker.sock $IMAGE /sbin/init 1> /dev/null

echo "Begin pull image:"

docker pull $IMAGE

docker run --ulimit nofile=655350:655350 --cap-add SYS_ADMIN --device /dev/fuse --ulimit memlock=-1 --cpu-shares=15360 --cpu-quota=9600000 --cpu-period=100000 --memory=500000m -d --net=host --name $name -v $HOME:$HOME -v $CUR_DIR/.ssh/:$HOME/.ssh -v $CUR_DIR/after_start.sh:/tmp/after_start.sh  -v $CUR_DIR/.passwd:/tmp/.passwd -v $CUR_DIR/.group:/tmp/.group -v /etc/sysconfig/network:/etc/sysconfig/network:ro -v /etc/sysconfig/network-scripts:/etc/sysconfig/network-scripts:ro -v /etc/hosts:/etc/hosts:ro -v /etc/yum.repos.d/:/etc/yum.repos.d.mount/:ro -v /var/run/docker.sock:/tmp/docker.sock $IMAGE /sbin/init 1> /dev/null

if [ $? -ne 0 ]; then
    echo "ERROR, run container failed, please check."
    exit 3
fi

sshme="#!/bin/bash\nssh $USER@`hostname` -o ConnectTimeout=1 -p $SSHPORT \$*"
[ -e $CUR_DIR/sshme ] && rm -rf $CUR_DIR/sshme
echo -e $sshme > $CUR_DIR/sshme
chmod +x $CUR_DIR/sshme
dockerstop="#!/bin/bash\ndocker stop $name;docker rm -fv $name"
stopSh=stopContainer.sh
[ -e $CUR_DIR/$stopSh ] && rm -rf $CUR_DIR/$stopSh
echo -e #!/bin/bash\n$dockerstop > $CUR_DIR/$stopSh
chmod +x $CUR_DIR/$stopSh

echo "INFO start container success"
