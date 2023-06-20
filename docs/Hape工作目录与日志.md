# 集群工作目录
* domain daemon工作目录（默认情况下位于hape tool所在机器的 ~/havenask/hape/admin/domains）
    * heartbeats/\<domain-name\>/\<role\>/\<worker-name\>为一个domain某一个role的状态文件
        * \<worker-name\>-user-target.json为当前用户执行命令后的记录，会迅速被domain daemon消费
        * \<worker-name\>-final-target.json为domain daemon生成的最终命令，会发给worker daemon消费
        * \<worker-name\>-heartbeat.json为worker返回给domain daemon的实时状态
    * logs 为一个domain的hape tool日志
        * \<domain-name\>/admin-daemon.log为domain daemon的日志
        * \<domain-name\>/shell.log 为domain daemon执行的shell命令日志（包含本地和远程）
* worker daemon工作目录（默认位于worker机器的~/havenask/hape/workers/\<worker-name\>）
    * package 为worker的装包目录
    * heartbeats 包含分发给worker的final-target和domain daemon会扫描的实时状态heartbeat文件
    * hape-logs/worker-daemon.log 为worker daemon日志
    * hape-logs/shell.log 为worker执行的shell命令日志（包含本地和远程）
* havenask worker工作目录（与worker daemon工作目录路径一致）
    * <data> 源数据路径（仅bs worker存在）
    * <runtimedata> 索引路径（仅bs和searcher worker存在）
    * <worker_config> biz config路径
    * <local_search_xxx> 在线引擎详细信息

# hape日志与havenask引擎日志
## domain日志：admin-daemon.log
* 在一个domain日志中的每一行中，有关于具体某个worker的目标被不同标识字段区分。在排查问题时可以利用这一点
    * 有关某个worker的target处理日志标识字段为[\<worker-name\>-target]
    * 有关某个worker的heartbeat处理日志标识字段为[\<worker-name\>-heartbeat]
## worker日志：worker-daemon.log
* 在worker日志中一般将一个目标划分为多段执行。处理一段目标的日志一般以solve xxx为开始，以solve xxx succeed为结束

## shell日志：shell.log
* 在一个shell日志中的每一行中，有关于具体某个worker的目标被不同标识字段[\<worker-name\>-target]区分。在排查问题时可以利用这一点
* 每一个shell命令的具体内容，以及返回的return code、输出、错误都会写入日志中
* 由于shell命令可以区分为本地命令和远程命令，因此每行日志还包含[\<ip\>-\<timestamp\>]的标识符
* 部分shell执行错误会抛出错误，因此可以用ERROR进行搜索

## 引擎日志：ha3.log/bs.log
* bs的引擎日志位于<worker工作目录>/bs.log下
* 在线的引擎日志位于<worker工作目录>/local_search_xxx/<zone>/logs/ha3.log下