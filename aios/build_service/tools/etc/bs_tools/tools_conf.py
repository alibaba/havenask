import os

curPath = os.path.split(os.path.realpath(__file__))[0]

install_prefix = os.path.realpath(curPath + '/../../')
lib_path = install_prefix + "/lib64/:" + install_prefix + "/lib/:" \
           + os.path.realpath(install_prefix + "/../lib/") + ":" \
           + os.path.realpath(install_prefix + "/../lib64/") + ":" \
           + "/ha3_depends/usr/local/lib64:/ha3_depends/usr/local/lib:/ha3_depends/usr/lib64/:/ha3_depends/usr/lib/"\
           + "/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib:"\
           + "/opt/taobao/java/jre/lib/amd64/server/"

python_path = [os.path.realpath(install_prefix + "/../ali/lib/python2.7/site-packages/"),
               os.path.realpath(install_prefix + "/lib/python2.7/site-packages/"),
               os.path.realpath(install_prefix + "/lib/python/site-packages/"),
               "/usr/ali/lib/python2.5/site-packages/",
               "/usr/local/lib/python/site-packages/"]

vipclient_set_failover_path = "/tmp"
java_home = "/opt/taobao/java/"
hadoop_home = "/home/hadoop.devel/hadoop-2.6.2-cdh5.4.0-etao4/hadoop/hadoop/"
hadoop_user = "hadoop"
hadoop_groups = "supergroup"
hadoop_replica = "3"
