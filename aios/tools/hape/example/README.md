# Example
* 可以使用以下命令运行所有example case。样例默认默认使用一张直写表测试，可以在命令后面加上--type offline使用全量表测试
```
/ha3_install/example/common/case.py run --case <样例名>
```

* 在样例运行完毕后，集群是不会删除的。可以使用以下命令继续与集群交互。例如normal例子运行完后，执行以下命令查看集群表状态
```
/ha3_install/example/common/case.py command --case normal --command "havenask gs table"
```
* 可以继续进行数据读写，例如
```
/ha3_install/sql_query.py --query "select * from in0" 
```


## 已经支持的样例


| 样例名  |  样例说明|
|---|---|
| normal | 默认样例，执行简单sql查询  |
| vector | havenask向量索引样例 |
| custom_analyzer| 以开源c++版jieba分词器为基础的定制havenask分词插件样例 |


## Example目录结构说明
* data 测试数据
* common 测试工具脚本目录
* cases 运行样例
* cases/\<case\> 具体某个样例
  * setup.py 样例运行元信息
  * xx_schema.json 某个样例的表schema
  * xx_conf 某个样例的定制集群配置