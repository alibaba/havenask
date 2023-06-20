<a name="QYo3Y"></a>
## 描述
TVF是Table Value Function的缩写。<br />TVF在TuringSQL中用来表示一类可以对整张表进行操作的函数集合。其的输入由一个或者若干个标量参数和一个SQL（可以理解为一张表）组成；与之对应的输出是一张表。


<a name="jExuk"></a>
## 语法格式
```sql
SELECT:
  SELECT [ DISTINCT ]
    { * | projectItem [, projectItem ]* }
  FROM Table({TVF})
    [ WHERE booleanExpression ]
    [ GROUP BY { groupItem [, groupItem ]* } ]
    [ ORDER BY { orderByItem [, OrderByItem ]* }]
    [ HAVING booleanExpression ]
    [ LIMIT number]
    [ OFFSET number]

TVF:
	{TVF Name}({scalar parameter}*, {SELECT})
```
<a name="iQ5cd"></a>
## 示例
<a name="ZOs8X"></a>
#### 简单的TVF使用
```sql
SELECT
    *
FROM
    TABLE (
        one_part_tvf(
            'rtp_url',
            123,
            (
                SELECT i1, i2, d3, d4 FROM t1
            )
        )
    )
```
<a name="L4Ex2"></a>
#### TVF嵌套
TVF目前不允许按照如下的方式嵌套：
```sql
SELECT
    *
from
    TABLE (
        tvf1(
            tvf2(
                (
                    select
                        *
                    from
                        t1
                )
            )
        )
    )
```
但允许下面的方式：
```sql
SELECT
    *
FROM
    TABLE (
        one_part_tvf_enable_shuffle(
            'rtp_url',
            123,
            (
                SELECT
                    i1, i2
                FROM
                    TABLE (
                        one_part_tvf_enable_shuffle(
                            'rtp_url_2',
                            234,
                            (
                                SELECT i1, i2, d3, d4 FROM t1
                            )
                        )
                    )
            )
        )
    )
```

<a name="cLMVl"></a>
## 内置TVF
[查看](https://yuque.antfin-inc.com/aios/sql/aq97qe)