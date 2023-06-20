# 集群状态查看
* 想要查看集群运行状态，有两种方式，一种是在集群工作目录查看日志文件、状态文件(见[Hape工作目录与日志
](https://github.com/alibaba/havenask/wiki/Hape%E5%B7%A5%E4%BD%9C%E7%9B%AE%E5%BD%95%E4%B8%8E%E6%97%A5%E5%BF%97))，一种是调用gs和gt子命令

# gs子命令结果格式（heartbeat）
以下字段并非一开始就有，而是随着集群逐步启动而产生
* status：表示集群状态，在线和离线worker不一样
    * 在线： preparing（准备环境中），starting（启动中），running（成功启动）
    * 离线：preparing（准备环境中），starting（启动中），building（构建索引中)，finished（表示全量完成）
* ipaddr：worker部署的机器
* last-heartbeat-timestamp 表示心跳时间戳，如果引擎启动成功后长期不更新心跳，说明daemon或者引擎进程有异常
* config_path表示引擎运行所依赖的biz配置
* local_config_path 表示引擎依赖的biz配置下载的本地路径，与config_path表示的远端路径相对应
* bs特有字段：
    * index_name 索引名称
    * index_path 索引路径
* qrs特有字段：
    * qrs_http_port qrs查询端口
    * subscribe_info 在--level detail选项下特有字段，表示qrs挂载的searcher服务信息，只需关注其中的default_sql_biz
* searcher特有字段：
    * index_info 表示当前searcher已经加载的索引，index_info的格式为index_name到index_path的映射
    * subscribe_info 在--level detail选项下特有字段，表示searcher对外服务信息，只需关注其中的default_sql_biz
* gs命令在--level detail选项下的特有字段：
    * starter：表示拉起引擎的脚本命令


```
{
    "bs": {
        "havenask-auction-index_in1-0-0-0.65535": {
            "status": "finished", 
            "config_path": "xxx:/home/yyy/hape/conf/generated/auction/auction_biz_config/auction_offline_config", 
            "last-heartbeat-timestamp": "2023-03-21 17:21:27.643144", 
            "index_name": "in1", 
            "ipaddr": "aaa", 
            "index_path": "xxx:/home/yyy/hape/index/auction/in1/generation_1679382770"
        }, 
        "havenask-auction-index_in0-0-1-32768.65535": {
            "status": "finished", 
            "config_path": "xxx:/home/yyy/hape/conf/generated/auction/auction_biz_config/auction_offline_config", 
            "last-heartbeat-timestamp": "2023-03-21 17:21:29.007922", 
            "index_name": "in0", 
            "ipaddr": "bbb", 
            "index_path": "xxx:/home/yyy/havenask/hape/index/auction/in0/generation_1679383585"
        }, 
        "havenask-auction-index_in0-0-0-0.32767": {
            "status": "finished", 
            "config_path": "xxx:/home/xxx/hape/conf/generated/auction/auction_biz_config/auction_offline_config", 
            "last-heartbeat-timestamp": "2023-03-21 17:21:27.662418", 
            "index_name": "in0", 
            "ipaddr": "aaa", 
            "index_path": "xxx:/home/yyy/havenask/hape/index/auction/in0/generation_1679383585"
        }
    }
}
```

# gt子命令结果格式（final-target）
* plan：和hape cmd集群状态与目标中的\<role\>_plan.json基本一致
* ipaddr：表示该worker分派到哪个宿主机
* host_init：宿主机初始化相关
* user_cmd：记录了最近执行的一条命令
* last-user-target-timestamp：最近执行的一条命令的时间
* bs特有字段：
    * generation_id 索引版本号
* qrs特有字段：
    * subscribe_info 当所有searcher启动成功后会出现，表示挂载的searcher服务信息集合。havenask由于只支持sql模式，所以只需关注其中的default_sql_biz

```
{
    "bs":{
        "havenask-auction-index_in1-0-0-0.65535":{
            "host_init":{
                "init-mounts":{
                    "/etc/hosts":"/etc/hosts",
                    "/home/yyy/havenask/hape/workers/havenask-auction-index_in1-0-0-0.65535/package":"/home/yyy/havenask/hape/workers/havenask-auction-index_in1-0-0-0.65535/package",
                    "/home/yyy/havenask/hape/workers/havenask-auction-index_in1-0-0-0.65535":"/home/yyy/havenask/hape/workers/havenask-auction-index_in1-0-0-0.65535"
                },
                "init-commands":[
                    "ln -s /ha3_install /home/yyy/havenask/hape/workers/havenask-auction-index_in1-0-0-0.65535/package/ha3_install"
                ],
                "init-packages":[
                    "xxx:/home/yyy//havenask/hape/conf/generated/auction/auction_hape_config/logging/worker_logging.conf",
                    "xxx:/home/yyy//havenask/hape/conf/generated/auction/auction_hape_config/global.conf",
                    "xxx:/home/yyy//havenask/hape/hape"
                ]
            },
            "user_cmd":"start-worker",
            "last-user-target-timestamp":"2023-03-21 15:12:50.134705",
            "domain_name":"auction",
            "worker_name":"havenask-auction-index_in1-0-0-0.65535",
            "plan":{
                "processor_info":{
                    "daemon":"python package/hape/domains/workers/worker_daemon.py --domain_name auction --role_name bs --worker_name havenask-auction-index_in1-0-0-0.65535 --global_conf package/global.conf",
                    "workdir":"/home/yyy/havenask/hape/workers/havenask-auction-index_in1-0-0-0.65535",
                    "envs":{

                    },
                    "image":"registry.cn-hangzhou.aliyuncs.com/havenask/ha3_runtime:0.3.0",
                    "args":[

                    ],
                    "ipaddr":"aaa",
                    "domain_name":"auction",
                    "replica_id":0,
                    "worker_name":"havenask-auction-index_in1-0-0-0.65535",
                    "role_name":"bs",
                    "packages":[

                    ]
                },
                "index_config":{
                    "partition_count":1,
                    "config_path":"xxx:/home/yyy//havenask/hape/conf/generated/auction/auction_biz_config/auction_offline_config",
                    "data_path":"xxx:/home/yyy//havenask/hape/conf/templates/data",
                    "index_name":"in1",
                    "replica_count":1,
                    "partition_id":0,
                    "generation_id":1679382770,
                    "replica_id":0,
                    "output_index_path":"xxx:/home/yyy/havenask/hape/index/auction/in1",
                    "realtime_info":{
                        "bootstrap.servers":"ccc",
                        "topic_name":"ddd"
                    }
                },
                "host_ips":[
                    "aaa",
                    "bbb"
                ]
            },
            "role_name":"bs",
            "type":"final-target",
            "last-final-target-timestamp":"2023-03-21 15:12:50.642073"
        }
    }
}
```
