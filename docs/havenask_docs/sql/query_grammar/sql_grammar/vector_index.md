---
toc: content
order: 30336
---

# 向量查询
## 通用查询
```
query=select id from in0 where MATCHINDEX('index_name', '0.1,0.2,0.98,0.6;0.3,0.4,0.98,0.6...')

注：in0为要查询的表名，index_name为向量索引名，后面是要查询的向量。
```

## 指定top n查询
```
query=select id from in0 where MATCHINDEX('index_name', '0.1,0.2,0.98,0.6;0.3,0.4,0.98,0.6&n=10')

注：in0为要查询的表名，index_name为向量索引名，后面是要查询的向量，n指定向量检索返回的top结果数。
```

## 设定最低分阈值
```
query=select id from in0 where MATCHINDEX('index_name', '0.1,0.2,0.98,0.6;0.3,0.4,0.98,0.6&sf=0.8')

注：in0为要查询的表名，index_name为向量索引名，后面是要查询的向量，sf指定要过滤分数的阈值。如果schema中配置的search_type参数是ip(内积距离)时，内积距离分数低于4.0的doc会被过滤掉。当用户在schema中的search_type参数是l2(欧式距离)时，欧式距离分数高于2.0的doc会被过滤掉，之所以是2.0, 而不是4.0, 是从性能考虑, 引擎计算的分数是欧式距离的平方。
```

## 区分类目的查询
```
query=select id from in0 where MATCHINDEX('index_name', '16#0.1,0.2,0.98,0.6;0.3,0.4,0.98,0.6;1512#0.3,0.4,0.98,0.6&n=200')
// 查询的向量需要做urlencode
query=select id from in0 where MATCHINDEX('index_name', '16%230.1%2c0.2%2c0.98%2c0.6%3b1512%230.3%2c0.4%2c0.98%2c0.6%26n%3d200')
注：区分类目的情况下，参数值中需要指定类目id以及要查询的向量，类目id和向量之间使用'#'分隔(query中需要对'#'做URLEncode)，多个类目之间使用逗号分隔，多个向量之间以分号分隔。
```