---
toc: content
order: 4
---

# Built-in TVFs

## rankTvf
**rankTvf** is similar to the rank window function of SQL statements. This function is used to filter data after data is dispersed. The following sample code provides the prototype of the function:
```
rankTvf("group_key", "sort_key", "reserved_count", (sql))
```
**group_key**: the field that is used to disperse data. You can specify multiple fields or leave the parameter empty. Separate multiple fields with commas (,).
**sort_key**: the field that is used for sorting. Separate multiple fields with commas (,). The plus sign (+) indicates that the sorting order is increased, and the minus sign (-) indicates that the sorting order is decreased. The default value is increased. The sorting field cannot be empty.
**reserved_count**: the number of reserved items in each group. A negative number indicates that all items are reserved.
**sql**: sql statements to be broken

After **rankTvf** is used to disperse and filter the results of SQL statements, the output results are still sorted in the order in which the original results of SQL statements are sorted. The filtered rows are deleted from the output results.
Sample code
```sql
select * from table (
  rankTvf('brand','-size','1', (SELECT brand, size FROM phone))
)
order by brand
limit 100

```
## sortTvf
sortTvf is used to provide the local top K feature. For example, you can use the sortTvf function to sort the top K results returned by the searcher and perform join operations on the results. If you use an ORDER BY clause, the clause is pushed to the query record searcher (QRS) for join operations. ORDER BY clauses are used for global sort operations.
The following sample code provides the prototype of the function:
```
sortTvf("sort_key", "reserved_count", (sql))
```
**sort_key**: the field that you want to use to sort data. You can specify multiple fields. The plus sign (+) specifies that the system sorts data in ascending order. The minus sign (-) specifies that the system sorts data in descending order. By default, the system sorts data in ascending order. You cannot leave this field empty.
**reserved_count**: Number of reservations per group
**sql**: sql statements to be sorted
**Differences between sortTvf and rankTvf**: SortTvf changes the order of the rows in the table.
Example:
```sql
select * from table (
  sortTvf('-size','3', (SELECT brand, size FROM phone))
)
```
## topKTvf
topKTvf is used to provide the local top K feature. For example, you can use the topKTvf function to sort the top K results returned by the searcher and perform join operations on the results. If you use an ORDER BY clause, the clause is pushed to the query record searcher (QRS) for join operations. ORDER BY clauses are used for global sort operations.
The following sample code provides the prototype of the function:
```
topKTvf("sort_key", "reserved_count", (sql))
```
**sort_key**: the field that you want to use to sort data. You can specify multiple fields. The plus sign (+) specifies that the system sorts data in ascending order. The minus sign (-) specifies that the system sorts data in descending order. By default, the system sorts data in ascending order. You cannot leave this field empty.
**reserved_count**: Number of reservations per group
**sql**: sql statements to be sorted
**Differences between topKTvf and sortTvf**: The final result is disordered.
Example:
```sql
select * from table (
  topKTvf('-size','3', (SELECT brand, size FROM phone))
)
```
## identityTvf
identityTvf is mainly used to block some sql optimization, and the op itself only outputs the original output.
The following sample code provides the prototype of the function:
```
identityTvf((sql))
```
**sql**: SQL statements to be blocked
Example:
```sql
select * from table (
  identityTvf((SELECT brand, size FROM phone))
)
```


## unpackMultiValue
The unpackMultiValue function is used to unpack multi-value fields. This function is used when GROUP BY clauses include multi-value fields and you want to expand the group-based results of multi-value fields.
The following sample code provides the prototype of the function:
```
unpackMultiValue("unpack_keys", (sql))
```
**unpack_keys**: the name of the field that you want to unpack. You can specify multiple fields. Separate multiple fields with commas (,).
**sql**: sql statement to be flattened
**Note**:

1. The type of unpacked multi-value fields does not change. Each unpacked multi-value field contains only one value or the unpacked field is empty.
2. We recommend that you unpack a small number of multi-value fields. If you unpack a large number of multi-value fields, the system may become overloaded due to the cartesian product relationship between fields.

Sample code
```sql
select * from table (
  unpackMultiValue('desc,price,size', (SELECT desc, price, size FROM phone where size > 5.5))
)
```

## enableShuffleTvf
The enableShuffleTvf function is used to switch SQL statements from a TVF to the QRS. For example, SQL statements that need to be processed by the rankTvf function can be pushed down to a searcher by default. If an SQL statement in the rankTvf function contains enableShuffleTvf, the rankTvf function only runs on the QRS. The following sample code provides the prototype of the function:
```sql
enableShuffleTvf((sql))
```
The following sample code provides an example on how to use the enableShuffleTvf function:
```sql
select * from table (
 enableShuffleTvf((SELECT brand, size FROM phone))
)
```
## distinctTopNTvf
The distinctTopNTvf function is used to disperse and sort data. The following sample code provides the prototype of the function:
```sql
distinctTopNTvf('distinct_fields', 'sort_fields', 'topn', 'distinct_count', 'searcher_topn', 'searcher_distinct_count', (sql))
```
**distinct_fields:** The field used to scatter data. You can specify multiple fields. Separate multiple fields with commas (,). You can leave this field empty.
**sort_fields:** The field used for sorting. You can specify multiple fields. A plus (+) indicates that the sorting order is increased. A minus (-) indicates that the sorting order is decreased. The default value is increased. The sorting field cannot be empty.
**topn: the total number of reserved items.
**distinct_count:** The number of entries that are reserved for each dispersal group.
**searcher_topn: the total number of searchers in each column.
**searcher_distinct_count:** The number of entries reserved for each hash group on each column searcher.
**Calculation process:**

1. Sort by **sort_fields**.
2. Traverse each row, calculate the grouping by **distinct_fields**, if the number of groups is less than **distinct_count**, put the row into the output queue, the number of groups +1 , otherwise, put the row into the waiting queue.
3. If the number of rows in the output queue is greater than or equal to **topn**, the first **topn** rows of the output queue are returned. Otherwise, assuming that the number of rows in the output queue is n, the first **topn** -n rows of data in the candidate queue are combined with the output queue in an ordered queue and then output.
## partialDistinctTopNTvf
The function is the distinctTopNTvf, but only on searcher or qrs. The following sample code provides the prototype of the function:
```sql
partialDistinctTopNTvf('distinct_fields', 'sort_fields', 'topn', 'distinct_count', (sql))
```
## OneSummaryTvf

The OneSummaryTvf function is used to execute the one summary plug-in chain. This function is compatible with the original logic of the one summary plug-in in the HA3 plug-in platform.
```json
// Configure in sql.json

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
                "tvf_name": "tpp",                                          // The TVF name.
                "parameters": {
                    "config_path" : "pluginsConf/one_summary.json",         // The configuration of the one summary plug-in.
                    "schema_path" : "schemas/mainse_summary_schema.json",   //The configuration of the summary schema.
                    "chain" : "tpp_default"                                 // The summary chain of the TVF.
                }
            }
        ]
    }
```
You can use the OneSummaryTvf function to configure multiple TVF prototypes. Each prototype represents a one summary plug-in chain.
```sql
tpp(
    'dl:tpp_default',                                      // kvpair                                
    '',                                                    // The query clause. In most cases, the query clause is not required.
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
By default, the summary plug-in only processes the original input fields. If you want to add fields, register the information about additional fields in iquan. You can register the information about additional fields by using the collectOutputInfo interface of the summary plug-in. If you do not register the information, the results of additional fields are not provided.
```cpp
typedef std::vector<std::pair<std::string, std::string>> SummaryOutputInfoType;
virtual SummaryOutputInfoType collectOutputInfo();

// example:
SummaryOutputInfoType PostFeeSummaryExtractor::collectOutputInfo() {
    if (_dstSummaryField.empty()) {
        return {};
    }
    return {make_pair(_dstSummaryField, "string")};    // [(field name,type)]
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

- _By which field the **group_fields:** is grouped, supports multiple columns, and uses; to divide_
- The **actual_group_field_values:** _enumerates all actual values corresponding to group_fields. If group_fields is a single column, all enumerated values are divided with; to indicate multiple groups; if group_fields is multiple columns, all enumerated values are divided with; if group_fields is multiple columns, all enumerated values are divided with; if group_fields is multiple columns, all enumerated values are divided with, to indicate multiple groups. _
- **quota_num:** _the quota of each group, which is described in two stages (for example, 10,20). 10 is the amount of recall data in the first stage (Searcher), and 20 is the amount of recall data in the second stage. A group is represented by two data, divided by,, and the number of groups here should correspond to the number of actual_group_field_values groups. If it is less than, the number of the last group is used as the default value of the following groups. If it is greater than, an error is reported._
- The **rerank_fields:** _field used for splitting. Multiple columns are supported. Multiple columns are separated by using a;. If you do not need to break, fill in the blank string _here.
- **sort_fields:** the _user sorting field (such as + field0;-field1;field2) to support multiple columns, + indicates ascending order,--descending order, default is ascending order_
- **per_rerank_field_max_num:**, _the upper limit of each category_
- **window_size:** when _you need to break up the function, it means that when you break up, you use a window to achieve unfair break up between groups._
- **need_makeup:** whether _it is necessary to complete the quota number, t indicates that it is necessary, f indicates that it is not necessary, here is also a two-stage description (such as t;f) with; segmentation_



**Note:** Use between groups; split, use within groups, split, here also support multi-value type fields, enumerate multi-value type data is split with#

**Procedure**

1. The system calculates input_hash based on the enumerated values of the grouping field.
2. The system traverses data in the table, calculates the hash value of all data, and then matches the hash value with the value of the input_hash parameter. In this case, the groups that correspond to the enumerated group data are obtained.
3. The system disperses multiple groups that are obtained in Step 2.
   1. ~~whether rerank_fields needs to be scattered or not is judged according to whether rerank_fields is empty or not. if rerank_fields is empty, it is not necessary. in this case, the ~~~~per_rerank_field_max_num ~~~~and window_size can be set to 0 ~~
   2. ~~If you need to break up ~~
      1. Group data based on the value specified by the rerank_fields parameter.
      2. Sort data in groups based on the value of the sort_fields parameter.
      3. The groups are sorted preferentially based on the window_size parameter. The window_size parameter specifies the window size of a group. Data in groups is extracted for several times.
      4. If the extracted data from a group reaches the value of the per_rerank_field_max_num parameter, the system stops extracting data from the group. If the amount of data extracted from all groups reaches the upper limit, the system stops extracting data from all groups.
      5. After the system stops extracting data from all groups, you can obtain two sets of data. One set of data meets all the rules of the user. The other set of data is a backup set.
   3. ~~if you don't break up ~~
      1. ~~Sort the data in the group based on the sort_fields ~~
      2. ~~filter all groups based on the quota data, two data can be obtained, one is the set that meets the quota set by the user, and the other is the alternative set ~~
4. Specify whether to supplement the result to reach the quotas. If you want to supplement the result, append the data in the backup set to the result. If you do not need to supplement the result, discard the data in the backup set. The policy that is used to supplement data varies based on the sequence of a group in the enumerated values of the actual_group_field_values parameter. A smaller sequence number indicates a higher priority of data supplementation.

**Example**
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

- _By which field the **group_fields:** is grouped, supports multiple columns, and uses; to divide_
- The **actual_group_field_values:** _enumerates all actual values corresponding to group_fields. If group_fields is a single column, all enumerated values are divided with; to indicate multiple groups; if group_fields is multiple columns, all enumerated values are divided with; if group_fields is multiple columns, all enumerated values are divided with; if group_fields is multiple columns, all enumerated values are divided with, to indicate multiple groups. _
- _Quota per Group_ **quota_num:**
- **sort_fields:** the _user sorting field, such as + field0;-field1;field2. Multiple columns are supported. + indicates ascending order.-indicates descending order. Default value: ascending order. If this field is empty, no sorting is required._
- _Whether you need to complete the **need_makeup:** to the quota number, t indicates yes, f indicates no, here is also a two-stage description_

**Procedure**

1. The system calculates input_hash based on the enumerated values of the grouping field.
2. The system traverses data in the table, calculates the hash value of all data, and then matches the hash value with the value of the input_hash parameter. In this case, the groups that correspond to the enumerated group data are obtained.
3. The system disperses and supplements data for groups that are obtained in Step 2.
   1. The system sorts data in groups based on the value of the sort_fields parameter. If the sort_fields parameter is left empty, data is not sorted.
   2. The system filters data in all groups based on quotas. You can obtain two sets of data. One set of data reaches the specified quota. The other set of data is a backup set.
4. Specify whether to supplement the result to reach the quotas. If you want to supplement the result, append the data in the backup set to the result. If you do not need to supplement the result, discard the data in the backup set. The policy that is used to supplement data varies based on the sequence of a group in the enumerated values of the actual_group_field_values parameter. A smaller sequence number indicates a higher priority of data supplementation.

**Example**
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

- _By which field the **group_fields:** is grouped, supports multiple columns, and uses; to divide_
- The **actual_group_field_values:** _enumerates all actual values corresponding to group_fields. If group_fields is a single column, all enumerated values are divided with; to indicate multiple groups; if group_fields is multiple columns, all enumerated values are divided with; if group_fields is multiple columns, all enumerated values are divided with; if group_fields is multiple columns, all enumerated values are divided with, to indicate multiple groups. _
- _Quota per Group_ **quota_num:**
- **sort_fields:** the _user sorting field, such as + field0;-field1;field2. Multiple columns are supported. + indicates ascending order.-indicates descending order. Default value: ascending order. If this field is empty, no sorting is required._

**Procedure**

1. The system calculates input_hash based on the enumerated values of the grouping field.
2. The system traverses data in the table, calculates the hash value of all data, and then matches the hash value with the value of the input_hash parameter. In this case, the groups that correspond to the enumerated group data are obtained.
3. The system disperses multiple groups that are obtained in the second step. The system sorts data in groups based on the value of the sort_fields parameter. If the sort_fields parameter is left empty, data does not need to be sorted.
4. The system selects all groups based on the value of the quota_num parameter. If the data in a group is insufficient, the system supplements data by using the following logic:
   1. In the supplementation logic of each phase, data in each group is selected based on the value of the quota_num parameter.
   2. If some groups contain insufficient data after data is selected based on the value of the quota_num parameter, you need to select the group specified by the lastRow parameter to supplement the remaining data for the group. If the group contains insufficient data, the system skips the current phase. lastRow specifies the group that corresponds to the current phase in reverse order.
   3. The system continues selecting the remaining groups that contain data and supplements data for the groups. Take note that the groups specified by the lastRow parameter must be updated based on the value of the quota_num parameter.
   4. Repeat the preceding steps until all data is supplemented or until no data in all groups can be selected.

**Example**
```sql
simpleRerankTvf('trigger_type',
                '1;11',
                '100',
                '-final_score',
                (sql)
               )
```
## Limits

1. When you execute SELECT statements to query the TVF results, include table (xxxTvf()) in FROM clauses.
2. We recommend that you enclose SQL statements in TVFs in parentheses (). You can execute only one SQL statement at a time. For example, an error is reported for sql1 union all sql2. To resolve the issue, change the statement into select * from (sql1 union all sql2).
3. TVFs support only parameters of the STRING type.


