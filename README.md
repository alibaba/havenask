## 项目介绍
Havenask是阿里巴巴集团自研的搜索引擎，也是阿里巴巴内部广泛使用的大规模分布式检索系统，支持了包括淘宝、天猫、菜鸟、高德、饿了么、全球化在内整个阿里巴巴集团的搜索业务，为用户提供高性能、低成本、易用的搜索服务，同时具有灵活的定制和开发能力，支持算法快速迭代，帮助客户和开发者量身定做适合自身业务的智能搜索服务，助力业务增长。

此外，基于Havenask打造的行业AI搜索产品——阿里云OpenSearch，也将持续在阿里云上为企业级开发者提供全托管、免运维的一站式智能搜索服务，欢迎企业级开发者们试用。
## 核心能力
Havenask 的核心能力与优势，有以下几点：
* <strong>极致的工程性能</strong>：支持千亿级数据实时检索，百万QPS查询，百万TPS写入，毫秒级查询延迟与秒级数据更新。
* <strong>C++的底层构建</strong>：对性能、内存、稳定性有更高保障。
* <strong>SQL查询支持</strong>：支持SQL语法便捷查询，查询体验更友好。
* <strong>丰富的插件机制</strong>：支持各类业务插件，拓展性强。
* <strong>支持图化开发</strong>：实现算法分钟级快速迭代，定制能力丰富，在新一代智能检索场景下的支持效果优秀。
* <strong>支持向量检索</strong>：可通过与插件配合实现多模态搜索，满足更多场景的搜索服务搭建需求（待发布）。



## 开始使用
* 使用前确保已经安装和启动Docker服务
### 启动运行时容器
1. 克隆仓库并创建容器。其中DOCKER_NAME为指定的容器名
```
docker pull havenask/ha3_runtime:0.1
cd ~
git clone git@github.com:alibaba/havenask.git
cd ~/havenask/docker
## 如果是Linux环境执行以下指令
./create_container.sh <DOCKER_NAME> havenask/ha3_runtime:0.1
## 如果是Mac环境执行以下指令
./create_container_mac.sh <DOCKER_NAME> havenask/ha3_runtime:0.1
```
2. 以ssh的方式登陆容器，执行python脚本构建havenask/example/data下测试文档的索引并执行sql查询。其中USER为进入容器前的用户名 
```
cd ~/havenask/docker/<DOCKER_NAME>
./sshme
cd /home/<USER>/havenask/example/scripts
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
docker pull havenask/ha3_dev:0.1
cd ~/havenask/docker
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
cd ~/havenask/aios/indexlib
## -j为编译线程数
scons install -j30
## 执行ut，进入指定的目录
scons . -u -j20
```
