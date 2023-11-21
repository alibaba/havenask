---
toc: content
order: 103
---

# Query data from Pkey-value tables or Pkey-Skey-value tables
## Description
KV and KKV queries are used to support KV tables and KKV tables in havenask. Iquan manages the registration information of KV tables and KKV tables.

When you query data from Pkey-value tables and Pkey-Skey-value tables, specify a primary key. For more information about primary keys, see the "Limits" section in this topic.


## Examples
### 1. KV table
* In this example, the cat_id field is used as the primary key in the Pkey-value table named category.
```
SELECT cat_id, category_name FROM category where cat_id=2;

SELECT cat_id, category_name FROM category where cat_id in (2, 3);
```

### 2. KKV table

* In this example, the company_id field is used as the primary key in the Pkey-Skey-value table named company.

```

SELECT company_id, company_name FROM company where company_id in (1,2);

SELECT company_id, company_name FROM company where company_id = 1 OR company_id = 2;
```

### 3. Restrictions
* KV and KKV single-table queries must contain the equivalent query condition of the PK field after the where clause.
* If the top-level query conditions are joined by using the AND operator, make sure that at least one sub-condition is the equivalent query condition of the primary key field.

```
SELECT company_id, company_name
FROM company
WHERE company_id IN (1,2) AND company_name <> 'HEHE';
```

* If the top-level query conditions are joined by using the OR operator, make sure that all sub-conditions have the equivalent query condition of the primary key field.

```
SELECT company_id, company_name
FROM company
WHERE
        (company_id = 1 AND company_name <> 'HEHE')
  OR
  (company_id IN (3,4,5) AND company_name = 'HEHE')
;
```

* If a Pkey-value table or Pkey-Skey-value table is associated with other tables, only LookupJoin can be used and the Pkey-value table or the Pkey-Skey-value table must be a built table.