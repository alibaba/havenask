---
toc: content
order: 4
---

# Hape Cluster Status
* This section mainly explains the result of the hape gs subcommand
* Note that in the following states, only the clusterStatus of sqlClientInfo in the havenask state is READY, which indicates that the final cluster is available. The READY status of a table can only indicate that the table is registered and does not indicate that the table is serviceable.

### havenask status
* Corresponding subcommand hape gs havenask
* The format is as follows. Meaning:
   * serviceZk indicates the ZK address where the service runs.
   * LeaderAlive indicates whether the admin is running normally.
   * hippoZk indicates the zk address scheduled by the service worker process.
   * processors indicates the process information of the admin and worker
      * role indicates the status of the node in the cluster. For example, two columns of searcher correspond to two database_partition_: 0 and database.database_partition_1. A role may have multiple identical machine services called replica
      * The role of havenask mainly includes admin, qrs, and searcher of multiple shards.
      * containerName indicates the name of the container on the machine where the container resides.
      * containerStatus indicates whether the container is running properly
      * processorStatus indicates whether the process is running properly
   * sqlClusterInfo indicates the status of the havenask worker node scheduling
      * replicaNodeId indicates the IDs of all replica nodes under a role.
      * slaveAddress indicates the node address.
      * clusterStatus indicates whether the cluster has reached the serviceable status.
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

### table status
* Corresponding subcommand hape gs table
* The format is as follows. Meaning:
   * status: indicates whether the table is ready.
   * tableName: the name of the table.
   * tableStructureConfig: table shards and types
      * shardCount Number of shards
      * shardFields primary key
      * buildType DIRECT indicates a write-through table. If this field does not exist, it indicates a full table.
   * columns: table fields
   * indexes: table indexes
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



### swift status
* Corresponding subcommand hape gs swift
* The format is as follows. Meaning:
   * serviceZk indicates the ZK address where the service runs.
   * LeaderAlive indicates whether the admin is running normally.
   * hippoZk indicates the zk address scheduled by the service worker process.
   * The meaning of processors is the same as that of processors in the havenask state.
   * Swift's role mainly includes admin and multi-shard broker
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


### bs status
* Corresponding subcommand hape gs bs
* The format is as follows. Meaning:
   * serviceZk indicates the ZK address where the service runs.
   * LeaderAlive indicates whether the admin is running normally.
   * hippoZk indicates the zk address scheduled by the service worker process.
   * The meaning of processors is the same as that of processors in the havenask state.
   * bs's role mainly includes the following, which will be dynamically pulled up according to the situation:
      * prepare_data_source full preparation node
      * processor.full full document preprocessing
      * builder.full full index building
      * merge. full full index merge
      * processor.inc Real-time Document Preprocessing
      * builder.inc Incremental index building
      * merge. inc Incremental index merge
   * When the full process is all finished and the inc process is pulled up, it means full completion.
   * buildInfo indicates the index task that is currently being built.
      * generationId indicates the sequence number of the task.
      * tableName indicates the table for which the index is being built
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
