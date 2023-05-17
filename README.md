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

* Havenask Docker images
  * <strong>ha3_runtime</strong>: runtime image allows you to quickly start a search service without compiling.
  * <strong>ha3_dev </strong>: development image includes all libraries necessary for compiling havenask. （[Wiki: Compile Code](https://github.com/alibaba/havenask/wiki/Get-Started-en#compile-code)）
* Requirements
  * Runtime: cpu > 2 cores, memory > 4G, disk > 20G
  * Dev: cpu > 2 cores, memory > 10G, disk > 50G 
  * Install and Start Docker




### Run the container

* clone the repository
```
git clone git@github.com:alibaba/havenask.git
```

* create the container. CONTAINER_NAME specifies the name of the container.
```
docker pull havenask/ha3_runtime:0.3.0
./create_container.sh <CONTAINER_NAME> havenask/ha3_runtime:0.3.0
```
* If the connection to Docker Hub is unstable, you can try to download the images from Alibaba Cloud Container Registry.
```
docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0
./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0
```



* Log on to the container.

```
./<CONTAINER_NAME>/sshme
```



### Build Demo Index
```
cd ~/havenask/example/
python build_demo_data.py /ha3_install
```

### Query Demo Index
* start havenask 
```
python start_demo_searcher.py /ha3_install
```
* havenask container listens on the default port of 45800. Here are some examples. ([More examples](https://github.com/alibaba/havenask/tree/main/example))


```
python curl_http.py 45800 "query=select count(*) from in0"

python curl_http.py 45800 "query=select id,hits from in0 where MATCHINDEX('title', '搜索词典')"

python curl_http.py 45800 "query=select title, subject from in0_summary_ where id=1 or id=2"

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
* 镜像介绍
   * ha3_runtime：可直接运行的镜像，无需编译代码，包含了问天引擎的可执行文件。
   * ha3_dev：用于开发测试的镜像，里面包含了开发测试时需要的各种依赖库和头文件，如何编译问天引擎请参考[编译代码](https://github.com/alibaba/havenask/wiki/%E5%BF%AB%E9%80%9F%E5%BC%80%E5%A7%8B#%E7%BC%96%E8%AF%91%E4%BB%A3%E7%A0%81)。
* 环境要求
   * 运行镜像：确保机器内存大于4G，cpu大于2核，磁盘大小大于20G。
   * 开发镜像：确保机器内存大于10G，cpu大于2核，磁盘大小大于50G。
   * 使用前确保设备已经安装和启动Docker服务。
 
### 启动容器
克隆仓库
```
git clone git@github.com:alibaba/havenask.git
cd havenask/docker
```
创建容器，其中CONTAINER_NAME为指定的容器名
```
docker pull havenask/ha3_runtime:0.3.0
./create_container.sh <CONTAINER_NAME> havenask/ha3_runtime:0.3.0
```
如果由于Docker Hub访问不稳定无法下载以上镜像，可以尝试阿里云镜像源
```
docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0
./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0
```
登陆容器
```
./<CONTAINER_NAME>/sshme
```

### 测试索引构建

构建全量索引
```
cd ~/havenask/example/
python build_demo_data.py /ha3_install
```

### 测试引擎查询
启动havenask引擎
```
python start_demo_searcher.py /ha3_install
```

引擎的默认查询端口为45800，使用脚本进行查询测试。下面是一些测试query。更多测试case见[example](https://github.com/alibaba/havenask/tree/main/example)

```
python curl_http.py 45800 "query=select count(*) from in0"

python curl_http.py 45800 "query=select id,hits from in0 where MATCHINDEX('title', '搜索词典')"

python curl_http.py 45800 "query=select title, subject from in0_summary_ where id=1 or id=2"
```

## 联系我们
官方技术交流钉钉群：

![3293821693450208](https://user-images.githubusercontent.com/590717/206684715-5ab1df49-f919-4d8e-85ee-58b364edef31.jpg)

