## 开始使用
* 使用前确保已经安装和启动Docker服务
### 启动运行时容器
1. 克隆仓库并创建容器。其中DOCKER_NAME为指定的容器名
```
docker pull havenask/ha3_runtime:0.1
cd ~
git clone git@gitlab.alibaba-inc.com:havenask/ha3.git
cd ~/ha3/docker
## 如果是Linux环境执行以下指令
./create_container.sh <DOCKER_NAME> havenask/ha3_runtime:0.1
## 如果是Mac环境执行以下指令
./create_container_mac.sh <DOCKER_NAME> havenask/ha3_runtime:0.1
```
2. 以ssh的方式登陆容器，执行python脚本构建ha3/example/data下测试文档的索引并执行sql查询。其中USER为进入容器前的用户名 
```
cd ~/ha3/docker/<DOCKER_NAME>
./sshme
cd /home/<USER>/ha3/example/scripts
## 对于测试文档构建索引
python build_demo_data.py /ha3_install
## 执行查询
python start_demo_searcher.py /ha3_install
```
## 编译ha3
### 编译环境
1. 请确保编译的机器内存在15G以上，mac编译时需调整Docker容器资源上限（包括CPU、Memory、Swap等），具体路径：Docker Desktop->setting->Resources。
2. 请确保cpu位8core以上，不然编译比较慢。
### 启动开发容器
* ha3镜像分为开发镜像和运行时镜像，前者用于编译ha3，后者无需编译即可运行ha3。本节使用的是开发镜像
```
docker pull havenask/ha3_runtime:0.1
cd ~/ha3/docker
## 如果是Linux环境执行以下指令
./create_container.sh <DOCKER_NAME> havenask/ha3_dev:0.1
## 如果是Mac环境执行以下指令
./create_container_mac.sh <DOCKER_NAME> havenask/ha3_dev:0.1
```
* 接下来创建开发容器的命令与上一节一致
  
### 执行编译指令
* 执行以下指令。如果报"fatal error: Killed signal terminated program cc1plus"错误，是由于编译机器内存不足导致的，请build时减少线程数（如：./build 5）
```
./configure
./build
```
### 单独编译子项目方法
* 以indexlib为例
```
cd ~/ha3/aios/indexlib
## -j为编译线程数
scons install -j30
## 执行ut，进入指定的目录
scons . -u -j20
```
