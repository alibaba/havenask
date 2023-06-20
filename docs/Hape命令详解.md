# hape命令格式
* hape包含多个子命令
    * 使用的格式为子命令名称+子命令参数
    * 大部分子命令不需要指定domain的名字，但是需要指定hape config的路径，因为hape cmd会从hape config的路径中解析domain的名字

```
python bin/hape_cmd.py <sub-command> --<sub-command-arguments> 
```

* 直接运行以下命令可以获取hape的帮助

```
python bin/hape_cmd.py
```

* 直接运行以下命令可以获取hape子命令的帮助

```
python bin/hape_cmd.py <sub-command> -h
```

# hape支持的子命令
## init 
* 作用：帮助用户创建一份基本的集群配置
* 说明：
    * 创建的配置包括
        * biz config，也即引擎实际加载的在离线配置，用户可以用户可以用生成的biz config配置运行其他子命令，也可以根据自己的业务需求调整biz_config，如何修改配置请参考[配置问天引擎](https://github.com/alibaba/havenask/wiki/%E9%85%8D%E7%BD%AE%E9%97%AE%E5%A4%A9%E5%BC%95%E6%93%8E)。
        * hape config，部署havenask集群的计划，计划中的引擎在线离线配置都指向上面的这份biz config。用户可以用生成的hape config配置运行其他子命令，也可以在该基础上根据自己的需求调整。配置格式见[hape配置详解](https://github.com/alibaba/havenask/wiki/Hape%E9%85%8D%E7%BD%AE%E8%AF%A6%E8%A7%A3)
* 必选参数
    * --domain_name：想要创建的havenask集群名称
    * --iplist：用户提供的集群物理机ip列表，格式为\<ip1\>,\<ip2\>,\<ip3\>
        * 分布式havenask的容器会平均分布在这些ip上。如果没有多台物理机，可以只填当前机器的ip，那么此时hape admin和worker都会在当前机器启动
        * 如果想要指定bs/searcher/qrs具体用哪些ip的机器，可以在init执行完后，基于[hape配置详解](https://github.com/alibaba/havenask/wiki/Hape%E9%85%8D%E7%BD%AE%E8%AF%A6%E8%A7%A3)中hostips参数说明进行调整
    * --index：想要在havenask集群中创建哪些名称的索引
        * 格式为index:part-count，表示创建名称为index的索引，分片数为part-count
        * 多个index:part-count用逗号隔开表示多表（多份索引）
        * 需要注意的是在多份索引情况下，假定最大的partition数为N，则每个索引的数会被限制必须等于1或者N
* 可选参数
    * 只有需要写入实时数据时需要的参数，可以不填，目前只支持kafka消息队列，使用时需要确保worker机器能联通kafka消息队列地址
    * --rt_addr：实时数据的消息队列地址
    * --index_rt_topics：每份索引对应的消息队列topic名，和--index参数里面的索引名一一对应，格式为\<index1-topic\>,\<index2-topic\>（注意，这个实时topic并非hape自动创建，而是需要消息队列中已经存在）
## check
* 作用：在启动之前，对于集群进行基础的校验，当存在异常时会抛出错误。校验包含但不局限于以下内容
    * 集群是否联通
    * 镜像是否拉取
    * 校验是否安装了必要的linux工具
    * 校验用户的hape配置中replica数与partition数是否与host_ips的行列数一致
* 必选参数
    * --config：hape config路径
## start
* 作用：按照hape config中描述的计划启动一个havenask集群或者集群中的部分
* 必选参数：
    * --config：hape config路径
    * --role：all/bs/searcher/qrs 操作指定类型的havenask workers    
* 可选参数 
    * --worker：当想要重启某一个worker时候可以指定该参数
* 说明
    * 执行该命令后可以用gs和gt子命令检测当前集群状态
    * 如果对于一个已经启动的role或者worker执行start，会在不删除容器的前提下发生重启
    * 当首次启动searcher没有索引时，domain daemon会自动将bs构建的索引分发到searcher上加载
    * 当前版本由于暂时还没有searcher自动同步最新服务信息到qrs的能力，因此searcher重启后qrs也必须重启
## stop
* 作用：停止havenask集群或者集群中的部分，会删除容器，但是不会删除工作文件
* 必选参数：
    * --config：hape config路径
    * --role：all/bs/searcher/qrs 操作指定类型的havenask workers    
* 说明
    * 执行该命令后可以用gs和gt子命令检测当前集群状态

## remove
* 作用：移除havenask集群或者集群中的部分，会删除容器和工作文件
* 必选参数：
    * --config：hape config路径
    * --role：all/bs/searcher/qrs 操作指定类型的havenask workers    
* 说明
    * 执行该命令后可以用gs和gt子命令检测当前集群状态

## gs (get status)
* 作用：获取havenask集群或者集群中的部分状态
* 必选参数：
    * --config：hape config路径
    * --role：all/bs/searcher/qrs 操作指定类型的havenask workers    
* 可选参数：
    * --level: summary 或 detail，表示简化状态和详细状态。默认为summary
* 说明：状态的格式见[hape cmd集群状态与目标](https://github.com/alibaba/havenask/wiki/hape%E9%9B%86%E7%BE%A4%E7%8A%B6%E6%80%81%E4%B8%8E%E7%9B%AE%E6%A0%87%E8%AF%A6%E8%A7%A3)

## gt (get target)
* 作用：获取havenask集群或者集群中的部分目标
* 必选参数：
    * --config：hape config路径
    * --role：all/bs/searcher/qrs 操作指定类型的havenask workers    
* 说明：目标的格式见[hape cmd集群状态与目标](https://github.com/alibaba/havenask/wiki/hape%E9%9B%86%E7%BE%A4%E7%8A%B6%E6%80%81%E4%B8%8E%E7%9B%AE%E6%A0%87%E8%AF%A6%E8%A7%A3)

## upc(update config)
* 作用：把hape cmd配置文件里\<role\>_plan.json最新修改的config_path上的biz config下发到havenask在线集群对应role的worker上。这个过程不会停止worker的容器，但是会暂时不可服务
* 必选参数：
    * --config：hape config路径
    * --role：searcher/qrs 操作指定类型的havenask workers（不支持bs，bs可以修改config后用start或build_full_index子命令重新运行）
* 说明
    * 新的config path必须与原来的config path不一样才会生效，仅仅是修改原有config path上的文件不会生效
    * 执行该命令后可以用gs和gt子命令检测当前集群状态

## build_full_index
* 作用：和start子命令在--role bs下含义一致（但是在多表情况下可以指定构建某一个索引），基于hape cmd配置文件中bs_plan.json指定的最新data_path产出新索引
* 必选参数：
    * --config：hape config路径
    * --index_name: 要重新构建的索引名称
* 说明：
    * 执行该命令后可以用gs和gt子命令检测当前集群状态，其中gt命令可以看到即将产出的版本号generation_id
    * 该命令不自动将索引下发searcher，需要结合upf子命令
    * 该命令会自动产生新的索引版本号

## upf (update full index)
* 作用：将某份索引更新到searcher上，searcher的容器不会停止，但是会暂时不可服务
* 必选参数
    * --config：hape config路径
    * --index_name：要更新的索引名字
    * --index_path: 格式为<要更新的索引所在ip地址>:<绝对路径>或者<hdfs路径>
* 说明
    * 执行该命令后可以用gs和gt子命令检测当前集群状态
    * 目前已经产出的索引列表可以在bs_plan.json指定的路径output_index_path上查看

## dp (deploy package)
* 作用：根据hape cmd配置文件里最新修改的packages装包重启容器（实际上用start命令也可以实现相同效果）
* 必选参数
    * --config：hape config路径
* 说明
    * 执行该命令后可以用gs和gt子命令检测当前集群状态
    * 包会安装在<worker工作目录>/package/下

## restart_daemon
* 作用：重启某个domain daemon
* 必选参数
    * --config：hape config路径
* 说明：domain daemon如果不调用kill_daemon总是常驻后台的，即使是调用了remove指令。因此当daemon代码发生修改时，需要调用这个指令更新代码

## kill_daemon
* 作用：杀死某个domain daemon
* 必选参数
    * --config：hape config路径
