## Overview

The child table of Havenask is generated and registered as a compound field that can be referred to as a struct. The name of the child table is the name of the compound field. The fields of the child table cannot be directly queried. When you query the child table, perform the UNNEST function to expand the table. This operation is semantically equivalent to the inner join operation that you can perform on a parent table and child tables.





## Syntax

```

select:

  SELECT [ ALL | DISTINCT ]

  { * | projectItem [, projectItem ]* }

  FROM tableName, UNNEST(tableName.subTableName)

```



## Examples

1. The required fields of the child table are returned.



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



2. The fields of the child table are filtered.



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



## Note:

If a WHERE clause contains fields of child table and the child table record that corresponds to a parent table record is empty in the query result, the parent table record is not returned by default.



If a WHERE clause does not contain the fields of child table, a left join operation is performed on primary table records and child table records in the query result by default. You can use a hint to change the join method. For more information, see [Hint](https://github.com/alibaba/havenask/wiki/Hint-en).



If you configured primary tables and child tables, you can separately query the parent table.