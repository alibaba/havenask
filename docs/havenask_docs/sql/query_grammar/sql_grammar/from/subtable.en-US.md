---
toc: content
order: 102
---

# Child table queries
## Description
The child table generated by the havenask is registered as a composite field (which can be interpreted as a struct). The name of the child table is the name of the compound field. The fields of the child table cannot be directly queried. When you query the child table, execute the UNNEST function to expand the table. This operation is semantically equivalent to the inner join operation that you can perform on a parent table and child tables.


## Syntax
```
select:
  SELECT [ ALL | DISTINCT ]
  { * | projectItem [, projectItem ]* }
  FROM tableName, UNNEST(tableName.subTableName)
```

## Examples
1. Return child table fields

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

2. Sub-table field filtering

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

## Usage notes
If a WHERE clause contains the conditions of child table fields and the child table record that corresponds to a parent table record is empty in the query result, the parent table record is not returned by default.

If a WHERE clause does not contain the conditions of child table fields, a left join operation is performed on parent table records and child table records in the query result by default. You can use a hint to change the join method. For more information, see [Hint](./Hint).

If you configure a parent table and a child table, you can separately query the parent table.