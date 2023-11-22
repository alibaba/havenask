---
toc: content
order: 102
---

# 子表查询
## 描述
havenask的子表生成是会注册为一个复合字段（可以理解为Struct）。子表表名即为该复合字段的字段名。目前不支持直接查询该子表字段。查询子表时需要通过UNNEST操作展开子表（语义上等价于主子表InnerJoin）。


## 语法格式
```
select:
  SELECT [ ALL | DISTINCT ]
  { * | projectItem [, projectItem ]* }
  FROM tableName, UNNEST(tableName.subTableName)
```

## 示例
1. 返回子表字段

```
SELECT
    field_int32,
    field_int32 + 1 as output,
    sub_id,
    sub_string
FROM
    simple4,
    UNNEST(simple4.sub_simple4_table)
WHERE
    field_int8 >= 2
```

2. 子表字段过滤

```
SELECT
    field_int32,
    field_int32 + 1 as output
FROM
    simple4,
    UNNEST(simple4.sub_simple4_table)
WHERE
    field_int8 >= 2
    AND sub_id <= 5
```

## 注意
如果WHERE语句中带有子表字段的条件，且查询结果中某条主表记录对应的子表记录为空，则默认不返回这条主表记录。

如果WHERE语句中不带有子表字段的条件，则查询结果的主表记录和子表记录join方式默认为LEFT_JOIN，可通过hint修改，具体见[Hint](./Hint)

配置主子表的场景下，支持单独查询主表。