---
toc: content
order: 0
---

# Limits
* High-performance Search Edition does not support DDL and DML statements.
* You must use an ORDER BY clause together with a LIMIT clause.
* If you use the UNNEST clause to perform a query on a child table, you cannot specify an asterisk (*) in the SELECT clause to select all fields in the table.
* SQL queries in OpenSearch Vector Search Edition do not support data of the unsigned type. We recommend that you do not specify a field of an unsigned type when you configure the schema in OpenSearch Vector Search Edition. OpenSearch Vector Search Edition may fail to convert unsigned values to values of supported data types.
* The JOIN operator can be executed on only the Searcher node. Make sure that the data that you want to join is stored in the locations that Searcher workers can access.
* When you use the UNION operator, make sure that the field names, field types, and number of fields that you specify in the left operand are the same as the field names, field types, and number of fields that you specify in the right operand.
* You cannot specify dates and value ranges in the key=value format. You can specify a date and a value range in the following format: `WHERE QUERY('consign_time', '[1, 10]')`.
