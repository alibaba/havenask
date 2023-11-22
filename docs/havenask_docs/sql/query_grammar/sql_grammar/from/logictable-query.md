---
toc: content
order: 104
---

# 逻辑表查询

## 描述 
用于构造多行数据，通常和内置TVF一起使用。对比使用VALUES关键词构造数据，这种方式的优势有： 
1. 有schema 
2. sql plan可cache 
## 示例 
### 注册逻辑表 
配置在Qrs的biz/sql/{bizname}_logictable.json，描述逻辑表schema。例如下面的配置注册了2个逻辑表。 

```
{
    "tables": [
        {
            "catalog_name": "default",
            "database_name": "item",
            "table_version": 2,
            "table_name": "trigger_table",
            "table_type": "json_default",
            "table_content_version": "0.1",
            "table_content": {
                "table_name": "trigger_table",
                "table_type": "logical",
                "fields": [
                    {
                        "field_name": "trigger_id",
                        "field_type": {
                            "type": "int64"
                        }
                    },
                    {
                        "field_name": "ratio",
                        "field_type": {
                            "type": "float"
                        }
                    }
                ],
                "sub_tables": [
                ],
                "distribution": {
                    "partition_cnt": 1,
                    "hash_fields": [
                        "trigger_id"
                    ],
                    "hash_function": "HASH64"
                },
                "join_info": {
                    "table_name": "",
                    "join_field": ""
                },
                "properties": {}
            }
        },
        {
            "catalog_name": "default",
            "database_name": "item",
            "table_version": 2,
            "table_name": "simple_table",
            "table_type": "json_default",
            "table_content_version": "0.1",
            "table_content": {
                "table_name": "simple_table",
                "table_type": "logical",
                "fields": [
                    {
                        "field_name": "int64_id",
                        "field_type": {
                            "type": "int64"
                        }
                    },
                    {
                        "field_name": "string_id",
                        "field_type": {
                            "type": "string"
                        }
                    }
                ],
                "sub_tables": [
                ],
                "distribution": {
                    "partition_cnt": 1,
                    "hash_fields": [
                        "int64_id"
                    ],
                    "hash_function": "HASH64"
                },
                "join_info": {
                    "table_name": "",
                    "join_field": ""
                },
                "properties": {}
            }
        }
    ]
}

```

### 使用逻辑表构造数据 
<strong>例1</strong> 

```
SELECT *
FROM table (
  inputTableTvf(
    '100,0.1;200,0.2',
    (SELECT trigger_id, ratio FROM trigger_table)
  )
)
```
|trigger_id(float)|ratio(float)|
|------|------|
|100|0.1|
|200|0.2|

<strong>例2</strong> 

```
SELECT *
FROM table (
  inputTableTvf(
    '?',
    (SELECT trigger_id, ratio FROM trigger_table)
  )
)
```
kvpair添加iquan.plan.prepare.level:rel.post.optimize;urlencode_data:true;dynamic_params:%5b%5b%27100%3a0.1%3b200%3a0.2%27%5d%5d

其中dynamic_params的参数值是urlencode过的[['100:0.1;200:0.2']]


<strong>例3</strong> 

```
SELECT *
FROM table (
  inputTableTvf(
    'aaa;bbb;ccc;ddd',
    (SELECT string_id FROM simple_table)
  )
)
```
|string_id(multi_char)
|------|
|aaa|
|bbb|
|ccc|
|ddd|
