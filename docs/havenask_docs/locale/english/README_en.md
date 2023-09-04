

## Overview

Havenask is a large-scale distributed search engine developed by and widely used in Alibaba Group. Havenask provides high-performance, low-cost and easy-to-use search services for the entire Alibaba Group, including Taobao, Tmall, Cainiao, Amap, Ele.me, and global services. Havenask also provides flexible customization and development capabilities. It supports fast algorithm iteration, and tailors intelligent search services for customers and developers to boost business growth.



Havenask powers Alibaba Cloud OpenSearch, an AI search product for enterprise developers. You are welcome to try out the one-stop, fully managed, and O&M-free intelligent search services.

## Core capabilities

Havenask has the following core capabilities and competitive advantages:

* <strong>Ultimate engineering performance</strong>: supports real-time search of hundreds of billions of data, millions of queries per second (QPS), millions of write transactions per second (TPS), query latencies in milliseconds, and data updates within seconds.

* <strong>Underlying structure built in C++</strong>: ensures better performance, larger memory, and higher stability.

* <strong>Support for SQL queries</strong>: supports SQL syntax to facilitate queries and provide a more user-friendly query experience.

* <strong>Diverse plug-ins</strong>: supports various service plug-ins and is highly scalable.

* <strong>Graphical development</strong>: fast iterates algorithms in minutes, provides a wide range of customization capabilities, and is applicable to the next-generation intelligent search requirements.

* <strong>Support for vector search</strong>: works together with plug-ins to support multimodal search. This satisfies requirements of custom search services specific to different scenarios (vector search will be supported in later versions).



## References

* Havenask Wiki: [https://github.com/alibaba/havenask/wiki](https://github.com/alibaba/havenask/wiki)



## Get started

Before you get started with Havenask, make sure that Docker is installed and started.



### Start a container

Clone a repository.

```

git clone git@github.com:alibaba/havenask.git

cd havenask/docker

```

Create a container. CONTAINER_NAME specifies the name of the container.

```

docker pull havenask/ha3_runtime:0.1

./create_container.sh <CONTAINER_NAME> havenask/ha3_runtime:0.1

```

If you cannot download the image because of unstable connection to Docker Hub, try Alibaba Cloud Container Registry.

```

docker pull registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.1

./create_container.sh <CONTAINER_NAME> registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.1

```



Log on to the container.

```

./<CONTAINER_NAME>/sshme

```



### Build a test index



Build a full index.

```

cd ~/havenask/example/scripts

python build_demo_data.py /ha3_install

```



### Test queries

Start Havenask.

```

python start_demo_searcher.py /ha3_install

```



Port 45800 is used by this engine in queries by default. Run the following sample script to test queries:  



```

python curl_http.py 45800 "query=select count(*) from in0"



python curl_http.py 45800 "query=select id,hits from in0 where MATCHINDEX('title', 'search dictionary')"



python curl_http.py 45800 "query=select title, subject from in0_summary_ where id=1 or id=2"

```
