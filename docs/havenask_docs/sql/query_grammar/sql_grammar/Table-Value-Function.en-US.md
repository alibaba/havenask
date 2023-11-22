---
toc: content
order: 30321
---

# Table-Value-Function
## Description
OpenSearch Retrieval Engine Edition supports table-valued functions (TVFs). <br />TVF is used in TuringSQL to represent a set of functions that can perform operations on the entire table. The inputs of a TVF include one or more scalar parameters and an SQL statement that specifies a table. The output of a TVF is a table.


<a name="jExuk"></a>
## Syntax
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
## Examples
<a name="ZOs8X"></a>
#### Simple TVF
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
#### TVF nesting
You cannot specify nested TVFs in the following format:
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
You can also nest TVFs in the following way:
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
## Built-in TVFs
[Built-in TVF list](../../../custom_function/buildin_tvf.md)**