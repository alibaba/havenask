### Description

Summary queries are compatible with the two-phase queries in Havenask. When the Havenask table is registered in Iquan, a summary table is also registered. The name of the summary table consists of the table name that is specified in the schema and the suffix. The default suffix is _summary_. You can change the suffix based on your business requirements. The summary table is required for summary queries.



Primary keys must be included in summary queries.





### Examples

```

SELECT brand, price FROM phone_summary_ WHERE nid = 8 or nid = 7



SELECT brand, price FROM phone_summary_ WHERE nid IN (7, 8, 9)

```



### Limits on query conditions

In a summary query, the primary key field must be specified after a `where` clause. In the preceding example, the primary key field of the `phone` table is `nid`. This field is used to obtain the required data records.



If the query condition contains the primary key field, you can also use the `AND` operator to add other conditions. This way, the returned records are filtered.



`SELECT brand, price FROM phone_summary_ WHERE nid IN (7, 8, 9) AND price < 2000`