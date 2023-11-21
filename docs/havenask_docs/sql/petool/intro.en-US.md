---
toc: content
order: -1
---

# Introduction to Hape
Havenask PE tools, abbreviated as hape, is a simple distributed O&M tool for havenask. you can use hape to start a distributed havenask cluster on one or more physical machines and perform basic O&M operations.

### terms
* cluster
   * cluster is the basic unit of hape management, including a havenask cluster and the swift and bs clusters on which it depends.
* hippo
   * hippo is the scheduling system of havenask and is located on the admin (also known as appmaster) of the havenask/swift/bs cluster.
* havenask cluster
   * havenask sql online query engine
   * qrs: the query node in the havenask cluster
   * searcher/database: data nodes in the havenask cluster
* suez catalog and cluster services
   * suez is the service framework of the havenask cluster and is located on the admin of the havenask cluster
   * Users manage tables through the catalog service of suez
   * You can use the cluster service of suez to manage query nodes (qrs) and data nodes (databases).
* swift cluster
   * MSMQ components of havenask
* bs (buildservice) cluster
   * havenask full-scale offline cluster construction
* Write-through tables
   * A direct write table is a table that does not have a full data source and directly creates and writes data to it.
   * For a write-through table, data is written through the qrs node, and the data is sorted by the searcher node.
   * Write-through tables are suitable for scenarios where the data structure is simple and the index construction overhead is low.
* Full scale
   * A full table is a table that has a full data source and needs to bypass the full data to build a full index and cut it online to provide services.
   * For a full table, the BS cluster starts an index building process to start the processor, builder, and merger processes respectively. Real-time data needs to be pushed to the corresponding topic (generally the same as the table name) of Swift, and it can take effect after being processed by the processor.
   * Full tables are suitable for scenarios where full data sources are available or data is complex, the index construction overhead is high, such as vector retrieval scenarios, or offline processing resources need to be flexibly adjusted when a large amount of data is updated.
### Hape Fundamentals

#### Process scheduling
* The user starts the admin process by using HAPE.
* The admin process automatically pulls up a series of worker processes based on the user's cluster and table targets, monitors and ensures that the worker processes continue to run.
* hape allows you to manage multiple clusters and isolate different clusters by using the zk path and container identifier determined by the service name in the configuration.

#### Table management
* The havenask admin provides two services: catalog and cluster
* Users access these two services through the hape service, which are used to manage the basic table information and table deployment information respectively.
* The hape interface encapsulates the access to catalog and cluster services. Users do not need to connect directly, but only need to call the hape command.

#### Index construction
* havenask sql tables are divided into direct-write tables (DIRECT tables) and full-scale tables (OFFLINE tables)
* In the direct write table, the user directly writes data from the havenask query node through the sql statement.
* In the full table, the user builds a full index by using the buildservice and builds a real-time index by using the swift subscription.


![Basic principles](../../_resources/hape.jpg)
