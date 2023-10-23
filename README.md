## Note
> old version:
> [1.0.0-beta](https://github.com/alibaba/havenask/tree/1.0.0-beta),
> [main-stable-0.4](https://github.com/alibaba/havenask/tree/main-stable-0.4)<br>
> The new and old main branches have a significant difference, and if the sync code branch produces errors, please make them consistent with the current main branch.

## Introduction

[![Leaderboard](https://img.shields.io/badge/HavenAsk-Check%20Your%20Contribution-orange)](https://opensource.alibaba.com/contribution_leaderboard/details?projectValue=havenask)

As a search engine developed by Alibaba Group, Havenask is a large-scale distributed information search system widely used within Alibaba Group. It provides the search services for business across the Alibaba Group such as Taobao, Tmall, Cainiao, Amap, Ele.me, and global services. It offers high-performance and easy-to-use search services with low costs. Havenask also provides flexible customization and development capabilities. Its algorithms can be fast iterated. This way, Havenask enables you to develop intelligent search service tailored for your business and empower business growth.



Havenask delivers the following benefits:



* <strong>Ultimate performance</strong>: ensures realtime search from hundreds of billions of data records and achieves millions of queries per second (QPS) and millions of writes per second (TPS). Havenask delivers queries in milliseconds and updates data in seconds.

* <strong>C++ underlying structure</strong>: ensures higher performance and better memory and stability.

* <strong>Supported SQL queries</strong>: offers a more user-friendly query experience.

* <strong>Rich plugins </strong>: supports various business plug-ins, which makes the engine highly scalable.

* <strong>Supported graphical development</strong>: allows you to iterate algorithms in minutes. In addition, a wide range of customization capabilities are available with excellent performance delivered for next-generation intelligent search.


## Documentation
* Havenask Wiki: https://github.com/alibaba/havenask/wiki

## Get Started

* Havenask Docker images (image platform: amd64)
  * <strong>ha3_runtime</strong>: runtime image allows you to quickly start a search service without compiling.
  * <strong>ha3_dev </strong>: development image includes all libraries necessary for compiling havenask. （[QuickStart](docs/havenask_docs/快速开始.md)）
* Requirements
  * Runtime: cpu > 2 cores, memory > 4G, disk > 20G
  * Dev: cpu > 2 cores, memory > 10G, disk > 50G
  * Install and Start Docker
  * Check that you can ssh to the localhost without a passphrase, ref[passphraseless ssh](docs/havenask_docs/Hape单机模式.md)

## Start Havenask Service
* Create the container
CONTAINER_NAME specifies the name of the container.
```
cd ~
git clone git@github.com:alibaba/havenask.git -b 1.0.0
cd ~/havenask/docker/havenask
docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:1.0.0
./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:1.0.0
```

* Log on to the container
```
./<CONTAINER_NAME>/sshme
```

* Start Havneask service
```
/ha3_install/hape start havenask
```
* Create table
```
/ha3_install/hape create table -t in0 -s /ha3_install/hape_conf/example/cases/normal/in0_schema.json -p 1
```

* Insert data
```
/ha3_install/sql_query.py --query "insert into in0 (createtime,hits,id,title,subject)values(1,2,4,'test', 'test')"
```
* Query
```
/ha3_install/sql_query.py --query "select * from in0"
```


## Contact Us
Havenask DingTalk Group：

![3293821693450208](https://user-images.githubusercontent.com/590717/206684715-5ab1df49-f919-4d8e-85ee-58b364edef31.jpg)





## 项目介绍

[![Leaderboard](https://img.shields.io/badge/HavenAsk-%E6%9F%A5%E7%9C%8B%E8%B4%A1%E7%8C%AE%E6%8E%92%E8%A1%8C%E6%A6%9C-orange)](https://opensource.alibaba.com/contribution_leaderboard/details?projectValue=havenask)

Havenask是阿里巴巴集团自研的搜索引擎，也是阿里巴巴内部广泛使用的大规模分布式检索系统，支持了包括淘宝、天猫、菜鸟、高德、饿了么、全球化在内整个阿里巴巴集团的搜索业务，为用户提供高性能、低成本、易用的搜索服务，同时具有灵活的定制和开发能力，支持算法快速迭代，帮助客户和开发者量身定做适合自身业务的智能搜索服务，助力业务增长。

此外，基于Havenask打造的行业AI搜索产品——[阿里云OpenSearch](https://www.aliyun.com/product/opensearch)，也将持续在阿里云上为企业级开发者提供全托管、免运维的一站式智能搜索服务，欢迎企业级开发者们试用。

支持与大语言模型结合，实现基于文档的问答系统，详情参考[llm](llm/README.md)

## 核心能力
Havenask 的核心能力与优势，有以下几点：
* <strong>极致的工程性能</strong>：支持千亿级数据实时检索，百万QPS查询，百万TPS写入，毫秒级查询延迟与秒级数据更新。
* <strong>C++的底层构建</strong>：对性能、内存、稳定性有更高保障。
* <strong>SQL查询支持</strong>：支持SQL语法便捷查询，查询体验更友好。
* <strong>丰富的插件机制</strong>：支持各类业务插件，拓展性强。
* <strong>支持图化开发</strong>：实现算法分钟级快速迭代，定制能力丰富，在新一代智能检索场景下的支持效果优秀。
* <strong>支持向量检索</strong>：可通过与插件配合实现多模态搜索，满足更多场景的搜索服务搭建需求。

## 相关文档
* Havenask Wiki: [https://github.com/alibaba/havenask/wiki](https://github.com/alibaba/havenask/wiki)

## 开始使用
* 镜像介绍（镜像平台: amd64）
   * ha3_runtime：可直接运行的镜像，无需编译代码，包含了问天引擎的可执行文件。
   * ha3_dev：用于开发测试的镜像，里面包含了开发测试时需要的各种依赖库和头文件，如何编译问天引擎请参考[快速开始](docs/havenask_docs/快速开始.md)。
* 环境要求
   * 运行镜像：确保机器内存大于4G，cpu大于2核，磁盘大小大于20G。
   * 开发镜像：确保机器内存大于10G，cpu大于2核，磁盘大小大于50G。
   * 使用前确保设备已经安装和启动Docker服务。
   * 单机模式需要确保本机能免密ssh登录自己，详情参考[免密ssh](docs/havenask_docs/Hape单机模式.md)

## 启动服务

* 创建容器
其中CONTAINER_NAME为指定的容器名
```
cd ~
git clone git@github.com:alibaba/havenask.git -b 1.0.0
cd ~/havenask/docker/havenask
docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:1.0.0
./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:1.0.0
```

* 登陆容器
```
./<CONTAINER_NAME>/sshme
```

* 启动havneask服务
```
/ha3_install/hape start havenask
```
* 创建表
```
/ha3_install/hape create table -t in0 -s /ha3_install/hape_conf/example/cases/normal/in0_schema.json -p 1
```

* 写入数据
```
/ha3_install/sql_query.py --query "insert into in0 (createtime,hits,id,title,subject)values(1,2,4,'测试', '测试')"
```
* 查询数据
```
/ha3_install/sql_query.py --query "select * from in0"
```

## 联系我们
官方技术交流钉钉群：

![3293821693450208](https://user-images.githubusercontent.com/590717/206684715-5ab1df49-f919-4d8e-85ee-58b364edef31.jpg)
