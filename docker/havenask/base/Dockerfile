FROM alibaba-cloud-linux-3-registry.cn-hangzhou.cr.aliyuncs.com/alinux3/alinux3
RUN yum install -y iputils procps wget unzip zip tar rsync openssh-server openssh-clients util-linux net-tools lsof wget
vim iotop bzip2 findutils git file
RUN yum install -y gcc g++ binutils-gold
RUN rm /usr/bin/ld && ln -s /usr/bin/ld.gold /usr/bin/ld
RUN yum install -y python2 python2-devel python2-pip python3 python3-devel python3-pip
RUN ln -s /usr/bin/pip2 /usr/bin/pip & ln -s /usr/bin/python2 /usr/bin/python
RUN yum install -y java-1.8.0-alibaba-dragonwell && mkdir /opt/taobao && ln -s
/opt/alibaba/java-1.8.0-alibaba-dragonwell-8.8.8.302.b1-1.al8 /opt/taobao/java
RUN ln -s /opt/taobao/java/bin/java /usr/bin/java
RUN yum install -y m4
RUN wget http://yum.tbsite.net/alios/7/os/x86_64/Packages/flex-2.5.37-3.1.alios7.x86_64.rpm && rpm -i
flex-2.5.37-3.1.alios7.x86_64.rpm
RUN wget http://yum.tbsite.net/alios/7/os/x86_64/Packages/bison-2.7-4.1.alios7.x86_64.rpm && rpm -i
bison-2.7-4.1.alios7.x86_64.rpm
RUN wget http://www.rpmfind.net/linux/centos/8-stream/AppStream/x86_64/os/Packages/byacc-1.9.20170709-4.el8.x86_64.rpm
&& rpm -i byacc-1.9.20170709-4.el8.x86_64.rpm
RUN yum install -y patch cpio zlib-devel patchelf libaio ncurses-compat-libs snappy
