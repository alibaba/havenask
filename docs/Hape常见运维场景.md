# 场景一：启停单索引/多索引集群
## 步骤1：初始化
1. 使用hape cmd子命令init初始化命令，其中index参数允许设置单份索引或者多份索引
2. 根据实际需求调整init初始化得到的\<role\>_plan.json。如果涉及修改partition数，还需要联动修改biz config中相关参数
3. 使用hape cmd子命令check校验集群是否存在异常配置

## 步骤2：启动集群
1. 使用hape cmd子命令start启动集群
2. 使用hape cmd子命令gs查看集群状态，如果bs worker的状态全部都为finished，qrs/searcher的状态全部都为runing说明集群启动成功
3. 查询qrs

## 步骤3：停止集群
1. 根据是否要删除工作目录，选择使用hape cmd子命令stop或者remove停止集群

# 场景二：管理多domain
一个搜索服务多个domain
* 出于服务稳定性的考虑，一个搜索服务一般会包含2个及以上的domain。两个domain的配置完全一致。当一个domain异常或者需要变更时将全部流量切换到另一个domain上
hape_cmd domain隔离
* hape cmd利用hape config的名字前缀对于不同domain进行隔离，解析的格式为xxxxx/<domain_name>_hape_config 。每一个domain的daemon互相隔离，因此可以满足以上需求
异常切流
* 当一个domain异常时，用户可以从自己的服务发现上将该domain的所有qrs的ip摘除，此时所有流量由另一个domain处理。待异常处理完毕后qrs ip重新加入服务发现
变更切流
* 当需要变更一个搜索服务时（配置、索引、安装包等），用户同样可以先将一个domain的qrs ip摘除，变更当前集群，完成后重新加入服务发现。然后再对另一个domain做同样的事情

# 场景三：实时写入与定期索引全量整理
* 当前版本的haveask还不支持自动索引整理，但是支持了实时数据写入。因此需要定期做索引全量重建，以防实时写满内存以及索引碎片化
## 步骤1：运行bs全量任务
1. 修改bs_plan.json中的data_path为当前要跑的全量任务源数据地址，利用hape cmd的子命令build_full_index启动bs构建全量索引。data_path指定的源数据路径支持多份数据文件，bs会合并为一份全量文件
2. 参考[bs_plan.json](https://github.com/alibaba/havenask/wiki/Hape%E9%85%8D%E7%BD%AE%E8%AF%A6%E8%A7%A3#bs_planjson)中的kafka_start_timestamp参数设定全量切换后从指定时间戳开始消费实时消息。实时消息详解见[example](https://github.com/alibaba/havenask/wiki/%E5%AE%9E%E6%97%B6%E5%8A%9F%E8%83%BD%E4%BD%BF%E7%94%A8%E6%96%87%E6%A1%A3)
3. 利用hape cmd的gt子命令可以看到新的全量的版本号generation_id，利用gs子命令可以查看该版本全量是否完成，以及完成后的索引产出路径

## 步骤2：下发全量索引
1. 假设当前搜索服务有两个domain：domain1和domain2
2. 先将所有流量切到domain2上，利用upf子命令将产出的索引下发到domain1的searcher上。此时可以不断用gs子命令查看下发是否完成
3. 然后将所有流量切到domain1上，做同样的事情
4. 完成以上操作后，将流量切换到domain1和domain2上

# 场景四：运行中集群的变更
* 以下的所有操作都需要基于场景二中有关变更切流的描述进行操作
## 变更biz config
1. 切流变更
2. 使用upc子命令下发\<role\>_plan.json中的新config（不支持bs）
3. 使用gs和gt子命令查看集群状态
4. 恢复服务

## 变更索引
1. 切流变更
2. 使用build_full_index命令基于bs_plan.json构建新索引
3. 使用gs和gt子命令查看集群状态
4. 当构建完成时分为两种情况：
    * 如果没有修改数据分片分布（如修改源数据路径、索引产出路径、bs worker的biz配置路径），使用upf子命令下发新索引路径
    * 如果修改了数据分片分布（如修改partition数、增加删除索引），则需要使用start命令重启searcher，见下一节
5. 使用gs和gt子命令查看集群状态
6. 恢复服务

## 其他引发容器重启的变更
* 其他变更包含但不局限于以下操作。这些操作需要重启容器进程更新（对于索引较大的集群，重启容器代价较大）
    * 修改宿主机ip列表
    * 修改进程启动参数
    * 修改进程的镜像和安装包
    * 修改数据分片分布的变更索引操作
* 步骤：
    * 切流变更
    * 使用start 子命令下发最新的\<role\>_plan.json
    * 使用gs和gt子命令查看集群状态
    * 注意：如果seacher被重启了，qrs也必须重启，否则还会保留旧的查询服务订阅信息
    * 恢复服务

# 场景五：集群部分工作节点异常
* 首先可以根据hape常见问题进行排查
* 如果无法恢复，可以切流，然后对于异常的domain的部分role或者部分worker用hape cmd子命令start进行重启
* 此外，gs子命令提供last-heartbeat-timestamp字段表示集群心跳上次汇报的时间戳，如果集群启动成功后长时间不更新，说明daemon或者引擎进程有异常