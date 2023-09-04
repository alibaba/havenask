## 描述
关于Hint的定义可以查看这篇[文档](https://en.wikipedia.org/wiki/Hint_(SQL))。为了增强SQL的可定制性，TuringSQL现已支持了Hint语义。

## 语法格式
```
Select:
  SELECT [/*+ HintName(params)  */]
    { * | projectItem [, projectItem ]* }
  FROM tableExpression [/*+ HintName(params)  */]
  
HintName: [a-zA-Z][a-zA-Z_]*
Params:
 				Identifier[, Identifier]
      |
      	Identifier=Identifier[, Identifier=Identifier]
Identifier: [a-zA-Z_][a-zA-Z_0-9]*        
```

## 内置Hint
目前内置了如下四种Hint

|Hint名字 | 含义 | 语法|
|-- | -- | -- |
|HASH_JOIN | 1) 本Hint作用于Table上2) 附带本Hint的Table参与Join时优先采用HashJoin，并且本Table优先为内层build表 | HASH_JOIN(tableName1， tableName2，...)|
|LOOKUP_JOIN | 1) 本Hint作用于Table上2) 附带本Hint的Table参与Join时优先采用LookupJoin，并且本Table优先为内层build表 | LOOKUP_JOIN(tableName1， tableName2，...)|
|NORMAL_AGG | 1) 指示Hint作用的Aggregate优先采用一阶段执行 | NORMAL_AGG(     distributionCheck='false' \| 'true'，     propScope='all')distributionCheck：是否检查当前分布，如果distributionCheck=’true‘， 但是Agg的Group Key不是分列字段，那么当前Hint失效。propScope：是否将当前Hint向下传播到其他的Agg节点。如果propSCope='all', 代表将传播到当前及其以下的agg节点。如果不填，那么代表当前Hint只作用在最近的Agg节点。|
|NO_INDEX | 1) 指示Hint作用的字段在Filter时优先不开启索引优化 | NO_INDEX(      tableName='t1',       fields='a, b, c')tableName：用来制定表名。fields: 用来制定不使用索引优化的字段列表。|


## 示例

### HASH_JOIN
```
SELECT
    /*+ HASH_JOIN(tj_relation)*/
    *
FROM
    (
        SELECT
            /*+ HASH_JOIN(tj_item_raw)*/
            *
        FROM
            (
                SELECT
                    sum(tj_item.id) as sum0
                FROM
                    tj_item
                GROUP BY
                    tj_item.id
            ) B
            JOIN tj_item_raw on B.sum0 = tj_item_raw.id   --> (1)
    ) D
    JOIN tj_relation on D.sum0 = tj_relation.item_id    --> (2)
    
(1)(2)两个join会被优化为HashJoin    
```

### LOOKUP_JOIN
```
SELECT
    /*+ LOOKUP_JOIN(tj_relation)*/
    *
FROM
    (
        SELECT
            /*+ LOOKUP_JOIN(tj_item_raw)*/
            *
        FROM
            (
                SELECT
                    sum(tj_item.id) as sum0
                FROM
                    tj_item
                GROUP BY
                    tj_item.id
            ) B
            JOIN tj_item_raw on B.sum0 = tj_item_raw.id  ---> (1)
    ) D
    JOIN tj_relation on D.sum0 = tj_relation.item_id   ---> (2)
    
(1)(2)两个join均为LookupJoin    
```

### NORMAL_AGG
```
SELECT
    /*+ LOOKUP_JOIN(tj_relation), NORMAL_AGG(distributionCheck='false', propScope='all')*/
    tj_relation.price
FROM
    (
        SELECT
            *
        FROM
            (
                SELECT
                    sum(tj_item.id) as sum0
                FROM
                    tj_item
                GROUP BY              ----> (1)
                    tj_item.id
            ) B
            JOIN tj_item_raw ON B.sum0 = tj_item_raw.id
    ) D
    JOIN tj_relation ON D.sum0 = tj_relation.item_id
GROUP BY                             ----> (2)
    tj_relation.price
    
(1)(2)两个Agg均为NormalAgg（不拆分两个阶段）
```

### NO_INDEX

```
SELECT
    /*+ LOOKUP_JOIN(tj_relation), NO_INDEX(tableName='tj_relation', fields='pk') */
    *
FROM
    (
        SELECT
            /*+ NO_INDEX(tableName='tj_item', fields='shop_id, reserve_price') */
            *
        FROM
            (
                SELECT
                    SUM(tj_item.id) as sum0
                FROM
                    tj_item
                    /*+ NO_INDEX(tableName='tj_item', fields='shop_id') */
                WHERE
                        tj_item.id = 100       
                    AND
                        tj_item.shop_id = 500   ---> (2)
                GROUP BY
                    tj_item.id
            ) B
            JOIN tj_item_raw on B.sum0 = tj_item_raw.id
    ) D
    JOIN tj_relation /*+ NO_INDEX(tableName='tj_relation', fields='item_id') */  on D.sum0 = tj_relation.item_id
WHERE
    tj_relation.item_id = 900 AND tj_relation.pk = 100   ---> (1)
    
(1)中的pk和item_id不是用索引
(2)中shop_id不是用索引
```

## 内置ATTR HINT
attr hint是为加速查询处理提供给op的一些辅助信息，主要用于查询的截断，批量的大小等设置，在多列的场景下比较有用。目前支持3种attribute hint
### SCAN_ATTR
为scan op传递信息，目前支持三个参数：
* localLimit：scan输出多少条停止。
* batchSize：scan每批次输出条数，一般与localLimit一起使用。默认是输出全部结果，batchSize由表大小自动计算。
* nestTableJoinType：nest table的join type，可选模式为 {'inner', 'left'} 两种，默认为left join模式。
```
SELECT /*+ SCAN_ATTR(localLimit='3',batchSize='2',nestTableJoinType='inner')*/ 
    company_id, company_name 
FROM
	  company
```

SCAN_ATTR也可写在表名之后(推荐方式)
```
SELECT 
    company_id, company_name 
FROM 
    company /*+ SCAN_ATTR(localLimit='3',batchSize='2',nestTableJoinType='inner')*/
```

此外，scan attr还支持按partition查询的功能，注意此功能是调试功能，不要依赖于此功能来做分列查询。
* hashValues: 根据hash values列表计算查询的partition
* partitionIds: 只查partitionIds列表中的partition, -1为访问全部partition
指定hash值查，查询a1, a2，会根据表配置的hash func计算出查询的partition id

```
SELECT /*+ SCAN_ATTR(hashValues='a1,a2')*/ company_id, company_name 
FROM company
```

指定列查，只查询第1，2两列
```
SELECT /*+ SCAN_ATTR(partitionIds='1,2')*/ company_id, company_name 
FROM company
```

### AGG_ATTR

为agg op传递信息，目前支持groupKeyLimit与stopExceedLimit两参数，分别是agg key的个数限制与达到限制后是否查询报错。默认key limit是20万，超过key limit查询自动报错。AGG_ATTR只允许出现在select之后。

AGG_ATTR也支持propScope参数用于表示传播范围，默认只作用于当前节点。

```
SELECT /*+ AGG_ATTR(groupKeyLimit='3',stopExceedLimit='false')*/ 
    company_id, company_name 
FROM 
    company 
group by 
    company_id, company_name
```

### JOIN_ATTR

以下只支持 lookup join, 用于lookup join的截断。

lookupTurncateThreshold是当lookup inner join成功的key数量达到这个值后join就结束。使用的场景如下：lookup join的input有序，只需要返加top k个有inner join结果的查询。

lookupBatchSize 每批参与lookup join的行数，默认为500。

JOIN_ATTR也支持propScope参数用于表示传播范围，默认只作用于当前节点。

```
SELECT /*+ JOIN_ATTR(lookupTurncateThreshold='30', lookupBatchSize='50')*/ 
    id, daogou.company_id, company_name 
FROM daogou 
      JOIN company 
      ON daogou.id = company.company_id
```

join_attr与scan_attr是可以一起使用的
```
SELECT /*+ JOIN_ATTR(lookupTurncateThreshold='3', lookupBatchSize='2')*/ 
    id, daogou.company_id, company_name 
FROM 
    daogou /*+ SCAN_ATTR(localLimit='5',batchSize='1')*/ 
    JOIN 
        company 
    ON 
        daogou.id = company.company_id
```

### left outer join默认值 

当左表无法join到右表数据时，右表字段会填默认值，join attr 支持修改默认值，每种类型支持一个默认值

```
SELECT /*+ JOIN_ATTR(defaultValue='INTEGER:10,VARCHAR:aa')*/ 
    id, daogou.company_id, company_name 
FROM daogou 
      JOIN company 
      ON daogou.id = company.company_id
```




