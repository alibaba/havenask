 # 介绍
Havenask PE tools，简称hape，是havenask的简易分布式运维工具。用户可以利用hape在单台或者多台物理机上拉起分布式havenask集群，并进行基础的运维操作。

# 名词解释
* domain
    * 一个havenask集群被称为一个domain，一个hape工具可以管理多个domain
* config
    * hape config：关于havenask集群部署计划的配置文件，一个domain有一个hape config
    * biz config：havenask引擎本身加载的配置，分为离线配置offline config和在线配置online config
* hape cmd
    * hape命令行工具，负责向domain daemon提交用户请求
* hape domain daemon
    * 每当用户使用hape cmd创建一个domain，hape cmd都会在所在机器后台启动一个domain daemon进程用于专门管理该domain。所有hape cmd命令都会在后台被domain daemon异步处理
* hape worker & worker daemon
    * hape worker运行所在的机器被称为hape worker
    * hape worker持续接受domain daemon下发的目标，帮助启动havenask worker并维护havenask worker的状态
* havenask worker
    * 搜索引擎本身，分为三种role：bs（离线索引构建），searcher（在线索引查询），qrs（合并分布式searcher查询结果）
* target    
    * user target：用户对于一个domain下发的命令
    * final target：domain daemon实际上对于worker下发的命令
* heartbeat
    * domain daemon从worker daemon处收集到的集群状态信息

# hape实现原理介绍
1. 用户使用hape cmd初始化一份hape config，并进行调整
2. 用户基于hape config使用hape cmd创建一个domain，此时后台自动创建一个domain daemon.用户对于这个domain的所有命令都实际上被domain ademon在后台异步处理
3. 当domain daemon得到一个user target后，对于target进行处理后得到final target，并下发给worker daemon。其中也有部分target（例如启动集群时searcher将自身服务信息挂载到qrs上）是domain daemon在循环过程中自动下发的，无需用户手动下发
4. worker daemon接收到domain daemon提供的target后会根据命令对于havenask worker进行不同的处理，并提供最新的worker状态信息heartbeat给domain daemon

![image](https://user-images.githubusercontent.com/38515034/226543781-e1b0a3f5-87ea-4f69-b398-bb3009aba4e4.png)



