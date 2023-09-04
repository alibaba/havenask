## Hape集群状态
* 本节主要解释hape gs子命令的结果
* 需要注意的是以下状态中只有havenask状态中的sqlClientInfo的clusterStatus为READY表示最终集群可服务。表的READY状态只能表示表注册完成，不代表可服务

## havenask状态
* 对应子命令hape gs havenask
* 格式如下，含义：
    * serviceZk 表示服务运行的zk地址
    * leaderAlive 表示admin是否正常运行
    * hippoZk 表示服务worker进程调度的zk地址
    * processors 表示admin和worker的进程信息
        * role表示该节点在集群中的地位。例如2列的searcher分别对应两个role：database_partition_0和database.database_partition_1。一个role下可能有多个相同的机器服务，称为replica
        * havenask的role主要有admin、qrs和多分片的searcher
        * containerName表示容器在所在机器上的名称
        * containerStatus表示容器是否正常运行
        * processorStatus表示进程是否正常运行
    * sqlClusterInfo 表示havenask worker节点调度的状态
        * replicaNodeId 表示某个role下的所有replica节点Id
        * slaveAddress 表示节点地址
        * clusterStatus 表示该集群是否达到可服务的状态
```
{
    "serviceZk": "xxx", 
    "leaderAlive": true, 
    "hippoZk": "xxx", 
    "processors": [
        {
            "ip": "xxx", 
            "processorName": "suez_admin_worker", 
            "role": "admin", 
            "containerName": "havenask-sql-local_appmaster", 
            "containerStatus": "RUNNING", 
            "processorStatus": "RUNNING"
        }, 
        ...
    ],
    sqlClusterInfo": {                                                                   
        "clusterStatus": "READY",                                                         
        "qrs": [                                                                          
            {                                                                             
                "replicaNodeId": "qrs.qrs_partition_0.0",                                 
                "workerStatus": "WS_READY",
                "serviceStatus": "SVT_AVAILABLE",                                         
                "healthStatus": "HT_ALIVE",                                               
                "slotId": {                                                               
                    "id": 1,                                                              
                    "declareTime": 1693527281,                                            
                    "slaveAddress": "xxx"                                        
                }
            }
        ],
        "database": {                                                                     
            "database_partition_0": [                                                     
                {                                                                         
                    "replicaNodeId": "database.database_partition_0.0",                   
                    "workerStatus": "WS_READY",                                           
                    "serviceStatus": "SVT_AVAILABLE",                                     
                    "healthStatus": "HT_ALIVE",
                    "slotId": {                                                           
                        "id": 0,                                                          
                        "declareTime": 1693527281,                                        
                        "slaveAddress": "xxx.8"                                    
                    }
                }                                                                         
            ]
        }
    },
}

```

## table状态
* 对应子命令hape gs table
* 格式如下，含义：
    * status: 表是否准备好
    * tableName: 表名
    * tableStructureConfig: 表分片和类型
        * shardCount 分片数
        * shardFields 主键
        * buildType DIRECT表示直写表，如果该字段不存在表示全量表
    * columns: 表字段
    * indexes: 表索引
```
{
    "status": "READY",               
    "tableName": "in0",                    
    "tableStructureConfig": {            
        "buildType": "DIRECT",       
        "shardInfo": {                           
            "shardFunc": "HASH",  
            "shardFields": [                    
                "id"                                               
            ],                                
            "shardCount": 1         
        },                  
        "tableType": "NORMAL"   
    },                        
    "indexes": [                           
        {                             
            "indexConfig": {                                       
                "indexFields": [     
                    {               
                        "fieldName": "id"
                    }                         
                ]     
            },              
            "name": "id",                     
            "indexType": "PRIMARY_KEY64"
        },
        ...
    ], 
    "operationMeta": {}, 
    "version": "3", 
    "databaseName": "database", 
    "columns": [
        {
            "type": "UINT32", 
            "name": "id"
        }, 
        ...
    ], 
    "catalogName": "catalog"
}
```



## swift状态
* 对应子命令hape gs swift
* 格式如下，含义：
    * serviceZk 表示服务运行的zk地址
    * leaderAlive 表示admin是否正常运行
    * hippoZk 表示服务worker进程调度的zk地址
    * processors 含义同havenask状态中的processors
    * swift的role主要有admin、和多分片的broker
```
{
    "serviceZk": "xxx", 
    "leaderAlive": true, 
    "hippoZk": "xxx", 
    "processors": [
        {
            "ip": "xxxx", 
            "processorName": "swift_admin", 
            "role": "admin", 
            "containerName": "xxx", 
            "containerStatus": "RUNNING", 
            "processorStatus": "RUNNING"
        }, 
        ...
    ]
}
```


## bs状态
* 对应子命令hape gs bs
* 格式如下，含义：
    * serviceZk 表示服务运行的zk地址
    * leaderAlive 表示admin是否正常运行
    * hippoZk 表示服务worker进程调度的zk地址
    * processors 含义同havenask状态中的processors
    * bs的role主要有以下，会根据情况动态拉起：
        * prepare_data_source 全量准备节点
        * processor.full全量文档预处理
        * builder.full 全量索引构建
        * merger.full 全量索引合并
        * processor.inc 实时文档预处理
        * builder.inc 增量索引构建
        * merger.inc 增量索引合并
    * 当full进程全部结束，inc进程拉起时代表全量完成
    * buildInfo 表示当前正在构建的索引任务
        * generationId 表示任务的序号
        * tableName 表示正在构建索引的表
```
{
    "serviceZk": "xxx",                   
    "leaderAlive": true,
    "hippoZk": "xxx",               
    "processors": [
        {
            "ip": "xxx",
            "processorName": "build_service_worker",                                      
            "role": "xxx",
            "containerName": "xxx",                    
            "containerStatus": "RUNNING",
            "processorStatus": "RUNNING"
        },
        ...
    ],
    "buildInfo": {
        "in1": {
            "buildId": {
                "partitionName": "in1_part",                                              
                "tableName": "in1",
                "generationId": "xxx",                                             
                "databaseName": "database",
                "catalogName": "catalog"
            },
            "current": {
                "buildState": "RUNNING",
                "configPath": "xxx"        
            },
            "target": {
                "type": "BATCH_BUILD",
                "buildState": "RUNNING",
                "configPath": "xxx"        
            }
        }
    }
}
```
