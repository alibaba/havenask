### Nested Loop Join
* 嵌套循环join，对两表进行笛卡尔积展开再过滤，主要用于非等值join。
```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  t1.id > t2.id
```

* <strong>说明</strong> 数据会膨胀，性能比较差
* <strong>说明</strong> 链接的表类型为KV/KKV/Summary时，该表的查询条件中必须包含PK等值条件，参考“[使用限制](https://github.com/alibaba/havenask/wiki/KV%E3%80%81KVV%E6%9F%A5%E8%AF%A2#3-%E4%BD%BF%E7%94%A8%E9%99%90%E5%88%B6)”。

### Hash Join
* 用于包含等值条件的join
* 将小表按连接键计算出一个hash表，然后从大表一条条抽取记录，计算hash值，根据hash到A表的hash来匹配符合条件的记录。

```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  t1.id = t2.id
```

* <strong>说明</strong> 默认生成的优化计划不一定是hash join，可通过[hint](https://github.com/alibaba/havenask/wiki/Hint#%E5%86%85%E7%BD%AEhint)指定hash join
* <strong>说明</strong> 链接的表类型为KV/KKV/Summary时，该表的查询条件中必须包含PK等值条件，参考“[使用限制](https://github.com/alibaba/havenask/wiki/KV%E3%80%81KVV%E6%9F%A5%E8%AF%A2#3-%E4%BD%BF%E7%94%A8%E9%99%90%E5%88%B6)”。


### Lookup Join
* 用于包含等值条件的join
* 将小表按连接键作为大表的查询条件，去召回大表纪录。

```
SELECT
  t1.id, t2.id
FROM
  tj_shop AS t1
JOIN
  tj_item AS t2
ON
  t1.id = t2.id
```


* <strong>说明</strong> 等值条件的一边是建索引的字段才可以开启lookup join优化
* <strong>说明</strong> 支持等值条件部分字段建索引
* <strong>说明</strong> iquan默认生成的优化计划不一定是lookup join，可通过[hint](https://github.com/alibaba/havenask/wiki/Hint#%E5%86%85%E7%BD%AEhint)指定lookup join
* <strong>说明</strong> 链接的表类型为KV/KKV/Summary时
  * 该表的查询条件中必须包含PK等值条件，参考“[使用限制](https://github.com/alibaba/havenask/wiki/KV%E3%80%81KVV%E6%9F%A5%E8%AF%A2#3-%E4%BD%BF%E7%94%A8%E9%99%90%E5%88%B6)”。
  * 或者，join的等值条件部分包含PK字段