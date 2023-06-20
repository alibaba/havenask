# 初始化集群
* 在开始前请阅读[hape介绍](https://github.com/alibaba/havenask/wiki/Hape%E5%88%86%E5%B8%83%E5%BC%8F%E5%B7%A5%E5%85%B7)了解基础概念
* 克隆havenask仓库

```
git clone git@github.com:alibaba/havenask.git
```

* 下载hape工具三方依赖。在[release页面](https://github.com/alibaba/havenask/releases)中选择最新的release版本。下载assests的hape_depends.tgz
```
tar -xzvf hape_depends.tgz
cp -r hape_depends havenask/hape/ 
cd havenask/hape
```

# 集群物理机环境准备
* hape分布式工具所在机器:
    * linux系统
    * 安装了python 2.7，并将python 2.7置为默认python（推荐使用conda或者virtualenv管理）
    * 确保当前机器能够ssh免密联通worker，可以使用ssh-copy-id指令实现
    * 确保安装了linux rsync指令用于分发文件
* hape worker机器(如果hape工具所在机器也作为worker，那么同样要满足以下条件): 
    * linux系统
    * 确保集群中每一个机器上都有与上述hape工具执行命令时所在机器相同的账号以及账号对应的home目录
    * 确保当前机器能够ssh免密联通hape分布式工具所在机器
    * 确保每一个机器上安装了docker，且使用上述账号执行docker命令时无需sudo
    * 确保每一台机器上安装了linux rsync指令用于分发文件
    * 在下文执行init命令后还要检查hape config的roles下的\<role\>_plan.json里面的image，确保每一台机器上都已经拉取了该镜像

# 生成集群目标
* 使用[命令详解](https://github.com/alibaba/havenask/wiki/Hape%E5%91%BD%E4%BB%A4%E8%AF%A6%E8%A7%A3)中的init子命令初始化集群配置。支持使用单台或多台物理机部署分布式。这里注意如果在hape分布式工具所在机器上也部署hape worker，那么需要确保该机器能够免密ssh登录自己

```
python bin/hape_cmd.py init --iplist <iplist>  --domain_name <domain_name> --index <index_name:partition_count> <realtime optional arguments>
```

* 执行以上指令后会在conf/generated/\<domain_name\>上生成一份配置，由以下两部分组成
    * havenask引擎配置：\<domain_name\>_biz_config 
    * hape配置：\<domain_name\>_hape_config，这个配置的路径是后面的子命令频繁要用的
* （可选）根据自己的业务需求调整biz_config，如何修改配置请参考[配置问天引擎](https://github.com/alibaba/havenask/wiki/%E9%85%8D%E7%BD%AE%E9%97%AE%E5%A4%A9%E5%BC%95%E6%93%8E)。
* （可选）根据具体情况可以调整hape配置。具体含义见《hape cmd配置文件》的hape配置一节

# 集群异常检测
* 执行以下命令检查集群配置是否有异常
```
## 如果提示docker有问题，且镜像都已经拉取，尝试清理已经创建的havenask异常镜像，然后执行sudo chmod 666 /var/run/docker.sock
## 如果提示一个ip到另一个ip执行命令错误，说明两台机器没有打通
python bin/hape_cmd.py check --config conf/generated/<domain-name>/<domain-name>_hape_config
```


# 启动集群
* 执行以下指令后，用户目标会被保存，并在后台被domain daemon被异步执行。由于是异步执行，因此一般每条运维指令都要等待一定时间

```
python bin/hape_cmd.py start  --role all --config conf/generated/<domain-name>/<domain-name>_hape_config
## 可以通过以下指令确定domain daemon已经被启动了
ps -aux | grep domain_daemon
```

# 查看集群状态
* 执行gs子命令查看集群状态，使用方法见[Hape命令详解](https://github.com/alibaba/havenask/wiki/Hape%E5%91%BD%E4%BB%A4%E8%AF%A6%E8%A7%A3)

```
python bin/hape_cmd.py gs  --role all --config conf/generated/<domain-name>/<domain-name>_hape_config
```

* 状态格式参考 [hape cmd集群状态与目标](https://github.com/alibaba/havenask/wiki/hape%E9%9B%86%E7%BE%A4%E7%8A%B6%E6%80%81%E4%B8%8E%E7%9B%AE%E6%A0%87%E8%AF%A6%E8%A7%A3)
* 集群状态的组织格式为role到worker的映射，worker名字的格式为\<worker-name-prefix\>-\<domain-name\>-\<role-name\>-\<replica-id\>-\<partition-id\>-\<partition-from\>.\<partition-to\>（多表bs情况下role-name替换为index_\<index-name\>），例如

```
{
    "searcher": {
        "havenask-order-searcher-0-0-0.32767": {
            "status": "preparing"
        }, 
        "havenask-order-searcher-0-1-32768.65535": {
            "status": "preparing"
        }
    }, 
    "qrs": {
        "havenask-order-qrs-0-0-0.65535": {
            "status": "preparing"
        }
    }, 
    "bs": {
        "havenask-order-index_in1-0-0-0.65535": {
            "status": "preparing"
        }, 
        "havenask-order-index_in0-0-1-32768.65535": {
            "status": "preparing"
        }, 
        "havenask-order-index_in0-0-0-0.32767": {
            "status": "preparing"
        }, 
        "havenask-order-index_in2-0-0-0.32767": {
            "status": "preparing"
        }, 
        "havenask-order-index_in2-0-1-32768.65535": {
            "status": "preparing"
        }
    }
}
```

* 在执行start之后一般一开始集群状态为preparing，表示正在装包和分发目标。之后status会转为starting，表示正在启动或等待依赖（配置、索引、订阅信息）。最后status转为finished（bs）和running（searcher&qrs）。qrs状态转为runnning即表示已经成功启动一个集群，可以执行查询命令

# 集群查询
* 执行以下指令进行sql查询。qrs-http-port可以用前文的gs子命令查看

```
python hape/utils/curl_http.py <qrs-ip>:<qrs-http-port> "select * from <index-name>"
```

# 实时写入
* 调用以下指令先往某张表中写入kafaka实时消息，然后再对qrs查询该表，可以发现多了一条id为99的数据
* 参数含义
    * rt_address 实时消息队列地址
    * rt_topicname 实时消息的topic
    * rt_partcnt 实时消息topic的分片数
    * doc_path 写入文档地址
    * pk_field 写入文档的主键

```
python hape/utils/kafaka_producer.py  --rt_address <kafka-address> --rt_topicname <topic-of-index> --rt_partcnt <kafka-topic-partition-count> --doc_path ../../conf/templates/rt_data/test.data --pk_field id

python hape/utils/curl_http.py <qrs-ip>:<qrs-http-port> "select * from <index-name>"
```