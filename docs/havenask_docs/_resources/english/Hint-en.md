### Overview

For more information about hints, see the [documentation](https://en.wikipedia.org/wiki/Hint_(SQL)). Havenask allows you to use hints in SQL statements to ensure that you can define SQL queries in a more effective manner based on your business requirements.



### Syntax

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



### Built-in hints

The following four built-in hints are used currently:



| Hint name | Meaning | Syntax |

|-- | -- | -- |

| HASH_JOIN | 1) This hint applies to a table. 2) HashJoin is preferred when a table with this hint executes a join and this table is preferred as an inner build table. | HASH_JOIN(tableName1, tableName2,...)|

| LOOKUP_JOIN | 1) This hint applies to a table. 2) LookupJoin is preferred when a table with this hint executes a join and this table is preferred as an inner build table. | LOOKUP_JOIN(tableName1, tableName2,...)|

| NORMAL_AGG | 1) The aggregation node that the hint applies to preferentially performs execution. | NORMAL_AGG( distributionCheck='false' \| 'true', propScope='all')distributionCheck specifies whether to check the current distribution. If you specify ddistributionCheck=’true‘ but the aggregation group key is not a routing field, the current hint is invalid. propScope: specifies whether to push the hint to other aggregation nodes. If you specify propSCope='all', the system pushes the hint to the current aggregation node and the downstream aggregation nodes. If you do not specify this parameter, the system pushes the hint to the nearest aggregation node. |

| NO_INDEX | 1) For fields that the hint applies to, index optimization is preferentially disabled during filtering. | NO_INDEX( tableName='t1', fields='a, b, c')tableName: used to specify the table name. fields: specifies the fields for which you want to disable the index optimization feature. |





### Examples



#### HASH_JOIN

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



Hash join hints are specified to optimize the two join operations.     

```



#### LOOKUP_JOIN

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



Lookup join hints are specified to optimize the two join operations.    

```



#### NORMAL_AGG

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



The aggregation operations are normal aggregation operations. The system does not split the aggregation operations into two phases.

```



#### NO_INDEX



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



In tj_relation.item_id = 900 AND tj_relation.pk = 100, the pk and item_id fields are not indexes.

In tj_item.shop_id = 500, the shop_id field is not an index.

```



### Built-in ATTR hints

You can use ATTR hints to pass specific auxiliary information to operators to accelerate queries. In an ATTR hint, you can specify settings such as the maximum number of data entries that you want the system to return and the number of data entries in each batch. If you query data from multiple fields, you can use ATTR hints to accelerate queries. Havenask V3 supports three types of ATTR hints.

#### SCAN_ATTR

You can use a SCAN_ATTR hint to pass information to the scan operator. You can include the following parameters in the SCAN_ATTR hint:

* localLimit: specifies the maximum number of data entries that the scan operator can return.

* batchSize: specifies the number of data entries that the scan operator can return in each batch. In most cases, you must use this parameter together with the localLimit parameter. By default, the scan operator returns all data entries that are obtained and the system automatically calculates the batch size based on the table size.

* nestTableJoinType: specifies a join method for nested tables. Valid values: inner and left. Default value: left.

```

SELECT /*+ SCAN_ATTR(localLimit='3',batchSize='2',nestTableJoinType='inner')*/

    company_id, company_name

FROM

	  company

```



We recommend that you specify a SCAN_ATTR hint next to the name of the table from which you want to query data.

```

SELECT

    company_id, company_name

FROM

    company /*+ SCAN_ATTR(localLimit='3',batchSize='2',nestTableJoinType='inner')*/

```



In addition, you can use the SCAN_ATTR hint in statements for partition-based queries. You can use this hint to perform partition-based queries to debug applications. You cannot use this hint to perform partition-based queries in a production environment.

* hashValues: specifies the fields based on which the system calculates hash values to identify the partitions from which the system can query the required data.

* partitionIds: specifies the IDs of the partitions from which you want to query data. If you specify -1 as the value of this parameter, the system scans all partitions.

   In the following sample code, the hashValues parameter is specified. The system uses the hash function that you specified for the table to calculate hash values based on the a1 and a2 values and returns the IDs of the partitions that the system identifies based on the hash values.



```

SELECT /*+ SCAN_ATTR(hashValues='a1,a2')*/ company_id, company_name

FROM company

```



In the following sample code, the partitionIds parameter is specified. The system queries data from Partition 1 and Partition 2.

```

SELECT /*+ SCAN_ATTR(partitionIds='1,2')*/ company_id, company_name

FROM company

```



#### AGG_ATTR



You can use an AGG_ATTR hint to pass information to the aggregation operator. In the AGG_ATTR hint, specify the following parameters: groupKeyLimit that specifies the maximum number of data groups that can be processed by using this hint and stopExceedLimit that specifies whether to report an error when the limit is reached. The default value of groupKeyLimit is 200000 and the default value of stopExceedLimit is true. You can specify a AGG_ATTR hint only after the SELECT operator.



In an AGG_ATTR hint, you can include the propScope parameter to specify a scope based on which the hint can take effect. By default, the hint can take effect only on the current node.



```

SELECT /*+ AGG_ATTR(groupKeyLimit='3',stopExceedLimit='false')*/

    company_id, company_name

FROM

    company

group by

    company_id, company_name

```



#### JOIN_ATTR



In the following examples, the AGG_ATTR hints are used to specify lookup joins and the maximum number of lookup join results that the system can return.



lookupTurncateThreshold: specifies the maximum number of results that the system can return after the system performs a lookup inner join operation. When the system obtains the maximum number of results, the system stops joining other values in the specified fields. You can include the lookupTurncateThreshold parameter in a JOIN_ATTR hint in the following scenario: The input values of lookup join operations are sorted based on their relevance scores and you want the system to return only the specified number of results after the system performs inner join operations on the groups of values whose relevance scores are high.



lookupBatchSize: specifies the number of data rows that you want to use for the lookup join operation for each batch. The default value is 500.



In a JOIN_ATTR hint, you can include the propScope parameter to specify a scope based on which the hint can take effect. By default, the hint takes effect on the current node.



```

SELECT /*+ JOIN_ATTR(lookupTurncateThreshold='30', lookupBatchSize='50')*/

    id, daogou.company_id, company_name

FROM daogou

      JOIN company

      ON daogou.id = company.company_id

```



You can specify a JOIN_ATTR hint and a SCAN_ATTR hint in the same statement.

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



#### Default values for left outer join operations



If the system cannot obtain values from specific fields in the right table when the system performs a left outer join operation, the system uses the default values of these fields. You can use a JOIN_ATTR hint to specify default values. You can specify only one default value for all fields of the same data type.



```

SELECT /*+ JOIN_ATTR(defaultValue='INTEGER:10,VARCHAR:aa')*/

    id, daogou.company_id, company_name

FROM daogou

      JOIN company

      ON daogou.id = company.company_id

```








