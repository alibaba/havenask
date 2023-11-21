### Description

You can query data from key-value tables and Pkey-Skey-value tables in Havenask. You can use Iquan to manage the registration information in key-value tables and Pkey-Skey-value tables.



When you query data from key-value tables or Pkey-Skey-value tables, specify the primary key. For information about the primary key, see the following "Limits" section:





### Examples

#### 1. Key-value tables

* Note that the primary key field of the category in the key-value table is cat_id in the example.

```

SELECT cat_id, category_name FROM category where cat_id=2;



SELECT cat_id, category_name FROM category where cat_id in (2, 3);

```



#### 2. Pkey-Skey-value tables



* Note that the primary key field of the company in the Pkey-Skey-value table is company_id.



```



SELECT company_id, company_name FROM company where company_id in (1,2);



SELECT company_id, company_name FROM company where company_id = 1 OR company_id = 2;

```



#### 3. Limits

* When you want to query data from a single key-value table or Pkey-Skey-value table, the where clause must contain the query condition that is equivalent to the value of the primary key field.

* If the top-level query conditions are joined by using the AND operator, make sure that at least one sub-condition is equivalent to the value of the primary key field.



```

SELECT company_id, company_name

FROM company

WHERE company_id IN (1,2) AND company_name <> 'HEHE';

```



* If the top-level query conditions are joined by using the OR operator, make sure that all sub-conditions are equivalent to the value of the primary key field.



```

SELECT company_id, company_name

FROM company

WHERE

        (company_id = 1 AND company_name <> 'HEHE')

  OR

  (company_id IN (3,4,5) AND company_name = 'HEHE')

;

```



* If a key-value table or Pkey-Skey-value table is associated with other tables, only LookupJoin can be used and the key-value table or the Pkey-Skey-value table must be the built table.