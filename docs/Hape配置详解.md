## 介绍
以下是执行hape的init子命令后生成的hape配置文件，用户可以在生成文件的基础上调整
## 全局配置 global.conf
* 配置示例
```
[hape-worker]
worker-name-prefix = havenask
logging-conf = package/worker_logging.conf
workdir = /home/xxx/havenask/hape/workers

[hape-admin]
daemon-process-interval-sec = 1
logging-conf = /home/xxx/havenask/hape/conf/generated/shop/shop_hape_config/logging/admin_logging.conf
enable-daemon-debug = False
enable-cmd-debug = False
workdir = /home/xxx/havenask/hape/admin

[container-service]
type = docker

[dfs]
type = local
```
* 路径：conf/generated/\<domain-name\>/global.conf
* 一般无需修改，hape全局使用的参数
* 参数含义：
    * hape-admin
        * logging-conf 表示日志绝对配置路径
        * workdir 表示工作目录
        * enable-daemon-debug 如果用户想要开发hape，打开这个选项可以让每次运行命令时重启admin daemon
        * enable-cmd-debug 用户执行hape_cmd时想要看hape_cmd的一些详细执行过程可以打开该选项
    * hape-worker
        * worker-name-prefix 生成worker名字的格式为\<worker-name-prefix\>-\<domain-name\>-\<role-name\>-\<replica-id\>-\<partition-id\>-\<partition-from\>.\<partition-to\>（多表bs情况下role-name替换为index_\<index-name\>）
        * logging-conf 表示日志配置相对路径
        * workdir 表示工作目录
        * container-service 
            * 部署havenask集群时使用的容器服务，目前只支持docker
    * dfs
        * havenask集群所使用的文件服务，目前支持hdfs和local（基于ssh rsync的文件分发），默认为local
        * 当dfs设置为local时，下文中的\<role\>_plan.json中的涉及文件路径的地方，路径格式为\<ip\>:\<绝对路径\>
        * 当dfs设置为hdfs时，需要设置hadoop-home和hadoop-java-home参数表示hadoop的路径和hadoop对应的jdk路径，并确保各个机器上hadoop home和java home都与配置文件中的一致。下文中的\<role\>_plan.json中的涉及文件路径的地方需要设置为hdfs地址。


## 宿主机初始化配置  host_init.json
* 配置示例
```
{
    "init-mounts": {
        "${workdir}": "${workdir}", 
        "${workdir}/package": "${workdir}/package", 
        "/etc/hosts": "/etc/hosts"
    }, 
    "init-commands": [
        "ln -s /ha3_install ${workdir}/package/ha3_install"
    ], 
    "init-packages": [
        "xxx:/home/yyy/havenask/hape/conf/generated/shop/shop_hape_config/logging/worker_logging.conf", 
        "xxx:/home/yyy/havenask/hape/conf/generated/shop/shop_hape_config/global.conf", 
        "xxx:/home/yyy/havenask/hape/hape"
    ]
}
``` 
* 路径：conf/generated/\<domain-name\>/host/host_init.json
* 一般无需修改，hape在初始化worker集群时执行的内容
* 参数含义：
    * init-mounts 挂载工作目录
    * init-commands 初始化bash命令
    * init-packages 初始化依赖包，一般是把hape worker工具上传worker集群


## 集群部署配置 \<role\>_plan.json
* 配置示例

```
{
    "host_ips": [
        "aaa", 
        "bbb"
    ], 
    "processor_info": {
        "envs": {}, 
        "image": "registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0", 
        "args": [], 
        "packages": []
    }, 
    "index_config": {
        "index_info": {
            "in0": {
                "partition_count": 2, 
                "data_path": "xxx:/home/yyy/havenask/hape/conf/templates/data", 
                "realtime_info": {
                    "bootstrap.servers": "ccc", 
                    "topic_name": "ddd"
                }, 
                "output_index_path": "xxx:/home/yyy/havenask/hape/index/shop/in0", 
                "config_path": "xxx:/home/yyy/havenask/hape/conf/generated/shop/shop_biz_config/shop_offline_config"
            }, 
            "in1": {
                "partition_count": 1, 
                "data_path": "xxx:/home/yyy/havenask/hape/conf/templates/data", 
                "realtime_info": {
                    "bootstrap.servers": "ccc", 
                    "topic_name": "fff"
                }, 
                "output_index_path": "xxx:/home/yyy/havenask/hape/index/shop/in1", 
                "config_path": "xxx:/home/yyy/havenask/hape/conf/generated/shop/shop_biz_config/shop_offline_config"
            }
        }
    }
}
```

* 路径：conf/generated/\<domain-name\>/roles/\<role\>_plan.json
* 一般用户会随着需求的迭代而频繁修改这部分文件
### bs_plan.json
* processor_info 拉起容器中进程所需信息
    * packages 拉起进程前装包
    * image 拉起进程镜像
    * envs & args：havenask进程启动参数和环境变量
* index_config bs工作配置
    * index_info：索引信息，key为索引名
    * partition_count：某一个表的分片数，这里需要注意所有表的最大分片数必须等于searcher的分片数，且所有表分片数不是等于这个最大的分片数就是等于1
    * config_path：某一个表的配置路径
    * data_path: 某个表的构建全量的源数据。data_path指定的源数据路径支持多份数据文件，bs会合并为一份全量文件
    * output_index_path: 某个表构建全量后索引的保存路径
    * realtime_info
        * bootstrap.servers：某一个表的实时kafka地址，该字段可选
        * topic_name：某一个表的实时kafka topic，该字段可选
        * kafka_start_timestamp: (可选参数，字符串类型)起始消费时间戳(us)，如果不传该参数默认从topic的起始开始消费
* hostips 分配在哪些worker机器上，是一个一维列表

## searcher_plan.json
* 配置示例
```
{
    "searcher_config": {
        "partition_count": 2, 
        "replica_count": 1, 
        "config_path": "xxx:/home/yyy/havenask/hape/conf/generated/shop/shop_biz_config/shop_online_config"
    }, 
    "processor_info": {
        "envs": {}, 
        "image": "registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0", 
        "args": [], 
        "packages": []
    }, 
    "host_ips": [
        [
            "aaa", 
            "bbb"
        ]
    ]
}
```
* processor_info 拉起容器中进程所需信息
    * packages 拉起进程前装包
    * image 拉起进程镜像
    * envs & args：havenask进程启动参数和环境变量
* searcher_config searcher工作配置
    * partition_count：分片数
    * replica_count：分片重复的行数
    * config_path：havenask在线配置路径
* hostips 分配在哪些worker机器上，是一个二维列表


## qrs_plan.json
* 配置示例
```
{
    "host_ips": [
        "aaa"
    ], 
    "processor_info": {
        "envs": {}, 
        "image": "registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0", 
        "args": [], 
        "packages": []
    }, 
    "qrs_config": {
        "replica_count": 1, 
        "qrs_http_port": 48000, 
        "config_path": "xxx:/home/yyy/havenask/hape/conf/generated/shop/shop_biz_config/shop_online_config"
    }
}
```  
*processor_info 拉起容器中进程所需信息
    * packages 拉起进程前装包
    * image 拉起进程镜像
    * envs & args：havenask进程启动参数和环境变量
* qrs_config qrs工作配置
    * replica_count：qrs个数
    * qrs_http_port：qrs查询接口，要保证一台机器上只有一个qrs，否则端口会冲突
    * config_path：havenask在线配置路径
* hostips 分配在哪些worker机器上，是一个一维列表（注意，不要在一个机器上部署多个qrs，否则会端口冲突！！！）

