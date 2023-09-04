import os

curPath = os.path.split(os.path.realpath(__file__))[0]
install_prefix = os.path.realpath(curPath + '/../../')
lib_path = install_prefix + "/lib64/:" + install_prefix + "/lib/:" \
           + os.path.realpath(install_prefix + "/../lib/") + ":" \
           + os.path.realpath(install_prefix + "/../lib64/") + ":" \
           + "/usr/local/lib64:/usr/local/lib:/usr/lib64:/usr/lib:/usr/ali/java/jre/lib/amd64/server/"
python_path = install_prefix + "/../ali/lib/python2.5/site-packages/" + ":" + "/usr/ali/lib/python2.5/site-packages/"
fs_util_exe = install_prefix + "/bin/fs_util"

hadoop_home = "/path/to/hadoop-0.20.2/"
#class_path_util = install_prefix + "/bin/class_path_util"
