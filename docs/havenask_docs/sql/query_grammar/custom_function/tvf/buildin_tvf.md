---
toc: content
order: 4
---

# 内置TVF

## rankTvf
**rankTvf**主要是为了提供类sql窗口函数rank的功能，用于打散后过滤，其原型如下：
```
rankTvf("group_key", "sort_key", "reserved_count", (sql))
```
**group_key**: 用于打散的字段，支持多个字段，使用,分隔， group_key可以为空。
**sort_key**：用于排序的字段，支持多个字段，通过逗号分隔，+表示增序，-表示减序，默认为增序，排序字段不允许为空。
**reserved_count**: 每组保留的个数， 负数为保留全部
**sql**: 需要打散的sql语句

**rankTvf**在打散过滤sql结果后，输出的结果仍保留原sql的顺序关系，但会删除被过滤掉的行。
使用示例：
```sql
select * from table (
  rankTvf('brand','-size','1', (SELECT brand, size FROM phone))
) 
order by brand 
limit 100 

```
## sortTvf
sortTvf主要是为了提供local topK的功能。例如是想在searcher上进行排序取topk的结果，再去做join。如果使用order by语句则会上推到qrs上做join，order by是全局的排序。
原型如下：
```
sortTvf("sort_key", "reserved_count", (sql))
```
**sort_key**：用于排序的字段，支持多个字段，+表示增序，-表示减序，默认为增序，排序字段不允许为空。
**reserved_count**: 每组保留的个数
**sql**: 需要排序的sql语句
**sortTvf与rankTvf的区别**：sortTvf会改变原sql的行在表中的顺序
使用示列：
```sql
select * from table (
  sortTvf('-size','3', (SELECT brand, size FROM phone))
)
```
## topKTvf
topKTvf主要是为了提供local topK的功能。例如是想在searcher上进行排序取topk的结果，再去做join。如果使用order by语句则会上推到qrs上做join，order by是全局的排序。
原型如下：
```
topKTvf("sort_key", "reserved_count", (sql))
```
**sort_key**：用于排序的字段，支持多个字段，+表示增序，-表示减序，默认为增序，排序字段不允许为空。
**reserved_count**: 每组保留的个数
**sql**: 需要排序的sql语句
**topKTvf与sortTvf的区别**：最终的结果无序
使用示列：
```sql
select * from table (
  topKTvf('-size','3', (SELECT brand, size FROM phone))
)
```
## identityTvf
identityTvf主要为了阻断一些sql优化，op自身只会原样的输出。
原型如下：
```
identityTvf((sql))
```
**sql**: 需要阻断的sql语句
使用示列：
```sql
select * from table (
  identityTvf((SELECT brand, size FROM phone))
)
```


## unpackMultiValue
unpackMultiValue的功能是多值字段的打平，主要用于group by中有多值字端时，需要多值字段展开group的场景。
原型如下：
```
unpackMultiValue("unpack_keys", (sql))
```
**unpack_keys**：需要展平的字段名，支持多个字段，字段间以','分隔
**sql**: 需要打平的sql语句
**注意**：

1. unpack多值后的字段类型不会变，但展平后的多值字段只包含0个或1个值
2. 展平的字段不要太多，字段间笛卡尔积关系，容易撑暴系统。

使用示例：
```sql
select * from table (
  unpackMultiValue('desc,price,size', (SELECT desc, price, size FROM phone where size > 5.5))
)
```
 
## enableShuffleTvf
enableShuffleTvf是让在此tvf之上的sql在qrs结点上跑，例如rankTvf，默认是可以下推到searcher，如果rankTvf中的sql包含了enableShuffleTvf, 此时rankTvf只会在qrs上跑。其原型如下：
```sql
enableShuffleTvf((sql))
```
使用方法：
```sql
select * from table (
 enableShuffleTvf((SELECT brand, size FROM phone))
) 
```
## distinctTopNTvf
distinctTopNTvf实现了打散和排序的功能。原型如下
```sql
distinctTopNTvf('distinct_fields', 'sort_fields', 'topn', 'distinct_count', 'searcher_topn', 'searcher_distinct_count', (sql))
```
**distinct_fields：**用于打散的字段，支持多个字段，使用逗号分隔，可以为空。
**sort_fields：**用于排序的字段，支持多个字段，+表示增序，-表示减序，默认为增序，排序字段不允许为空。
**topn：**总共保留的个数。
**distinct_count：**每个打散分组保留的个数。
**searcher_topn：**每列searcher上总共保留的个数。
**searcher_distinct_count：**每列searcher上每个打散分组保留的个数。
**计算流程：**

1. 按**sort_fields**排序。
2. 遍历每一行，按**distinct_fields**计算分组，如果分组个数少于**distinct_count**，将行放入输出队列，分组个数+1，否则，将行放入等待队列。
3. 如果输出队列行数大于等于**topn**，则返回输出队列的前**topn**行，否则假设输出队列行数为n，取候选队列前**topn**-n行数据跟输出队列做有序队列合并后输出。
## partialDistinctTopNTvf
功能同distinctTopNTvf，但只在searcher或qrs上执行。原型如下
```sql
partialDistinctTopNTvf('distinct_fields', 'sort_fields', 'topn', 'distinct_count', (sql))
```
## OneSummaryTvf

OneSummaryTvf用于执行one summary插件链，用于兼容原先ha3 插件平台one summary插件逻辑
```json
//sql.json中配置

"sql_tvf_plugin_config" : {
        "modules" : [
            {
                "module_name": "tvf",
                "module_path": "libPluginOnline.so",
                "parameters": {}
            }
        ],
        "tvf_profiles" : [
            {
                "func_name": "OneSummaryTvf",
                "tvf_name": "tpp",                                          // tvf配置名
                "parameters": {
                    "config_path" : "pluginsConf/one_summary.json",         // 指定one summary配置
                    "schema_path" : "schemas/mainse_summary_schema.json",   // 指定summary schema配置
                    "chain" : "tpp_default"                                 // 指定tvf对应summary chain
                }
            }
        ]
    }
```
OneSummaryTvf支持配置多个tvf原型，每个都代表一条one summary插件链
```sql
tpp(
    'dl:tpp_default',                                      // kvpair                                
    '',                                                    // query子句，一般不需要
    (                                                      // sql
        SELECT
            *
        FROM
            mainse_summary_summary_
        WHERE
            ha_in(nid, '620598126603|619969492623')
    )
)
```
hint
默认summary插件只处理原有输入字段，如果有额外字段需要新增，需要注册给iquan额外字段信息，可通过summary插件collectOutputInfo接口注册，不注册不会输出额外字段
```cpp
typedef std::vector<std::pair<std::string, std::string>> SummaryOutputInfoType;
virtual SummaryOutputInfoType collectOutputInfo();

// example:
SummaryOutputInfoType PostFeeSummaryExtractor::collectOutputInfo() {
    if (_dstSummaryField.empty()) {
        return {};
    }
    return {make_pair(_dstSummaryField, "string")};    // [(字段名,类型)]
}
```

---

## rerankByQuotaTvf
```sql
rerankByQuotaTvf('group_fields',
                 'actual_group_field_values',
                 'quota_num',
                 'rerank_fields',
                 'sort_fields',
                 'per_rerank_field_max_num',
                 'window_size',
                 'need_makeup',
                 (sql)
                )
```

- **group_fields：**_按照哪些字段进行分组，支持多列，用;分割_
- **actual_group_field_values：**_枚举出group_fields对应的所有实际值。如果group_fields是单列，则所有的枚举值用;分割来表示多组；如果group_fields是多列，则所有的枚举值组间用;分割，组内的多列的值用,分割_
- **quota_num：**_每个分组的quota数，这里分两阶段来描述，（比如10,20）10是第一阶段（Searcher）上的召回数据量，20是第二阶段的召回数据量，一个分组用两个数据来表示，用,分割，这里的组数和 actual_group_field_values的组数要对应上，如果少于则用最后一组的数来作为后边几组的默认值，如果大于，则报错_
- **rerank_fields：**_用于打散的字段，支持多列，多列之间用;分割。如果不需要打散，这里填空字符串_
- **sort_fields：**_用户排序的字段，（比如+field0;-field1;field2）支持多列，+表示升序，-表示降序，默认是升序_
- **per_rerank_field_max_num：**_需要打散功能的时候使用，表示组内打散的时候每个类目的上限数_
- **window_size：**_需要打散功能的时候使用，表示打散的时候采用窗口来实现组间的不公平打散_
- **need_makeup：**_是否需要补齐到quota数，t表示需要，f表示不需要，这里也是采用两阶段来描述（比如t;f)用;分割_



**备注：**组间用;分割，组内用,分割,这里也支持多值类型的字段，枚举多值类型的数据是用#分割

**执行流程：**

1. 根据用户枚举的分组字段值，计算input_hash
2. 遍历table数据，将所有的数据计算hash，然后和input_hash进行匹配，此时得到了用户枚举的分组数据对应的分组
3. 对第二步得到的多个分组，分别进行打散
   1. ~~需要打散与否，是根据rerank_fields是否为空判断，为空则不需要，此时~~~~per_rerank_field_max_num~~~~， window_size可设置为0~~
   2. ~~如果需要打散~~
      1. 根据rerank_fields进行分小组
      2. 根据sort_fields,小组内数据进行排序
      3. 对多个小组采用横向的定长（window_size）优先队列,抽取多轮
      4. 若小组内被抽取的数据达到per_rerank_field_max_num上限值，则对该小组停止抽取；若所有小组的抽取个数达到quota上限，则停止所有的抽取
      5. 该分组停止所有的抽取之后，可以得到两个数据，一个是符合用户的所有规则的集合，一个是备选集合
   3. ~~如果不要打散~~
      1. ~~根据sort_fields,小组内数据进行排序~~
      2. ~~对所有分组根据quota数据进行筛选，可以得到两个数据，一个是符合用户设置的quota的集合，一个是备选集合~~
4. 是否需要补齐quota数，如果需要，则把备选集合中的数据追加到最后，如果不需要，则把备选集合的数据丢弃（补齐的策略和actual_group_field_values枚举的顺序有关，越靠前则表示希望被优先补齐）

**举例说明：**
```sql
rerankByQuotaTvf('trigger_type',
                 '1;11',
                 '100,200;100,300',
                 'ju_category_id',
                 '-final_score',
                 '50',
                 '15',
                 't;t',
                 (sql)
                )
```

---

## simpleRerankTvf
```sql
simpleRerankTvf('group_fields',
                'actual_group_field_values',
                'quota_num',
                'sort_fields',
                'need_makeup',
                (sql)
               )
```

- **group_fields：**_按照哪些字段进行分组，支持多列，用;分割_
- **actual_group_field_values：**_枚举出group_fields对应的所有实际值。如果group_fields是单列，则所有的枚举值用;分割来表示多组；如果group_fields是多列，则所有的枚举值组间用;分割，组内的多列的值用,分割_
- **quota_num：**_每个分组的quota数_
- **sort_fields：**_用户排序的字段，（比如+field0;-field1;field2）支持多列，+表示升序，-表示降序，默认是升序，如果为空，则表示不需要排序_
- **need_makeup：**_是否需要补齐到quota数，t表示需要，f表示不需要，这里也是采用两阶段来描述_

**执行流程：**

1. 根据用户枚举的分组字段值，计算input_hash
2. 遍历table数据，将所有的数据计算hash，然后和input_hash进行匹配，此时得到了用户枚举的分组数据对应的分组
3. 对第二步得到的多个分组，分别进行打散补齐
   1. 根据sort_fields,小组内数据进行排序，如果为空，则不需要排序
   2. 对所有分组根据quota数据进行筛选，可以得到两个数据，一个是符合用户设置的quota的集合，一个是备选集合
4. 是否需要补齐quota数，如果需要，则把备选集合中的数据追加到最后，如果不需要，则把备选集合的数据丢弃（补齐的策略和actual_group_field_values枚举的顺序有关，越靠前则表示希望被优先补齐）

**举例说明：**
```sql
simpleRerankTvf('trigger_type',
                '1;11',
                '100',
                '-final_score',
                't',
                (sql)
               )
```

---

## TaobaoSpRerankTvf
```sql
taobaoSpRerankTvf('group_fields',
                'actual_group_field_values',
                'quota_num',
                'sort_fields',
                (sql)
               )
```

- **group_fields：**_按照哪些字段进行分组，支持多列，用;分割_
- **actual_group_field_values：**_枚举出group_fields对应的所有实际值。如果group_fields是单列，则所有的枚举值用;分割来表示多组；如果group_fields是多列，则所有的枚举值组间用;分割，组内的多列的值用,分割_
- **quota_num：**_每个分组的quota数_
- **sort_fields：**_用户排序的字段，（比如+field0;-field1;field2）支持多列，+表示升序，-表示降序，默认是升序，如果为空，则表示不需要排序_

**执行流程：**

1. 根据用户枚举的分组字段值，计算input_hash
2. 遍历table数据，将所有的数据计算hash，然后和input_hash进行匹配，此时得到了用户枚举的分组数据对应的分组
3. 对第二步得到的多个分组，分别进行打散，根据sort_fields,小组内数据进行排序，如果为空，则不需要排序
4. 对所有分组根据quota数据按照quota_num按比例进行挑选，若数据不足则需要补齐，补齐逻辑如下：
   1. 在每轮的补齐逻辑中，按照quota_num比例对每组数据进行挑选
   2. 如果当前轮次，按照比例挑选后，有部分group出现数据不足，则需要按照顺序挑选lastRow进行剩余数据的补齐，如果当前lastRow数据不足，则跳过本轮补齐（lastRow为与当前轮次逆序对应的group组）
   3. 对剩余含有数据的分组继续按照比例，挑选数据补齐，注意此时lastrow需要按照配置的quota逆序更新
   4. 重复直到所有数据补齐，或所有分组无数据可供挑选

**举例说明：**
```sql
simpleRerankTvf('trigger_type',
                '1;11',
                '100',
                '-final_score',
                (sql)
               )
```
## 注意事项

1. select tvf结果时，from后要加table (xxxTvf())
2. tvf中的sql语句最好用()引起来, 不支持多条sql的输出，例如 sql1 union all sql2这情况会报错，需要在嵌套一层select, 修改成select * from (sql1 union all sql2)
3. tvf中的参数目前只支持字符串类型的参数


