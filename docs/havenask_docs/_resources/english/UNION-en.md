### Description

You can use the UNION operator to merge the result sets that are obtained by using multiple SELECT clauses. The result sets that you want to merge must use the same schema. The fields that you specify in the left operand and in the right operand must be the same. The data types of the fields that you specify in the left operand must be the same as the data types of the fields that you specify in the right operand. The sequence in which you specify the fields in the left operand must be the same as the sequence in which you specify the fields in the right operand.



If you use the UNION ALL operator, the system includes duplicate values in the final result. If you use the UNION operator, the system returns a final result that does not include duplicate values. The UNION operator is used to perform an operation that is the same as the combination of the operations that are performed when you use the UNION ALL operator and the DISTINCT operator at the same time. The system requires a longer period of time to perform a UNION operation. We recommend that you use the UNION ALL operator.



If you want to generate a union of multiple result sets, you can use UNION or UNION ALL in the statement. You cannot specify UNION and UNION ALL in the same statement.



### Syntax



```

select_statement

	UNION [ALL]

	select_statement

   [UNION [ALL]

	select_statement];

```



### Examples

1. You can use the following sample code to merge two result sets:



```

SELECT nid, brand, price, size FROM phone WHERE nid < 5

UNION ALL

SELECT nid, brand, price, size FROM phone WHERE nid > 5

```



2. You can use the following sample code to merge more than two result sets:



```

SELECT

    nid, brand, price, size

FROM

    phone

WHERE nid >= 3 AND nid <= 5

UNION ALL

SELECT

    nid, brand, price, size

FROM

    phone

WHERE nid >= 6 AND nid <= 10

UNION ALL

SELECT nid, brand, price, size

FROM

    phone

WHERE nid >= 30 AND nid <= 40

```
