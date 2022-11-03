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
使用前确保已经安装和启动Docker服务

### 启动容器
克隆仓库并创建容器。其中DOCKER_NAME为指定的容器名
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
登陆容器
```
cd ~/havenask/docker/<DOCKER_NAME>
./sshme
```

### 测试索引构建

构建全量索引，其中USER为登陆容器前的用户名
```
cd /home/<USER>/havenask/example/scripts
python build_demo_data.py /ha3_install
```

### 测试引擎查询
启动havenask引擎
```
## <USER>为启动容器前的用户名
python start_demo_searcher.py /ha3_install
```

引擎的默认查询端口为45800，使用脚本进行查询测试。下面是一些测试query。

```
python curl_http.py 45800 "query=select count(*) from in0"

python curl_http.py 45800 "query=select id,hits from in0 where MATCHINDEX('title', '搜索词典')"

python curl_http.py 45800 "query=select title, subject from in0_summary_ where id=1 or id=2"
```


## 文档
* 更多详细的引擎使用说明见[havenask Wiki](https://github.com/alibaba/havenask/wiki)
