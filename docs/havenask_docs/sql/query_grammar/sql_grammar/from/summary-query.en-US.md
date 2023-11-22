---
toc: content
order: 101
---

# Summary queries
## Description
The summary query is compatible with the havenask two-phase query. When the havenask table is registered with Iquan, an additional summary table is registered. The name of the summary table is the name of the table that is configured in the schema and suffixed with the name of the table. The default value is_summary_, which is configurable. This table is required for summary queries.

Primary keys must be included in summary queries.


## Examples
```
SELECT brand, price FROM phone_summary_ WHERE nid = 8 or nid = 7

SELECT brand, price FROM phone_summary_ WHERE nid IN (7, 8, 9)
```

## Limits on query conditions
In a summary query, the primary key field must be specified after a `where` clause. In the preceding example, the primary key field of the `phone` table is `nid`. This field is used to obtain the required data records.

If the query condition contains the primary key field, you can also use the `AND` operator to add other conditions. This way, the returned records are filtered.

`SELECT brand, price FROM phone_summary_ WHERE nid IN (7, 8, 9) AND price < 2000`