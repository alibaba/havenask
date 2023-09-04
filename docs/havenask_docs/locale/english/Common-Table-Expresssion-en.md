## Overview

Havenask V3.7.5 and later versions support common table expressions (CTEs). You can create a CTE to specify a temporary result set that remains valid during the duration of a query. In SQL statements, you can reference the CTE to simplify the SQL statements. Havenask supports CTEs in only SELECT statements.



The following sample code shows the standard syntax of CTEs:



```

WITH expression_name[(column_name [,...])]

AS

   (CTE_definition)

SQL_statement;

```



Specify a name for the CTE that you want to create. In an SQL statement, you can specify the name to reference the CTE.



Specify output fields in parentheses () and separate multiple output fields with commas (,). Make sure that the number of output fields and the data types of the output fields are the same as the number of output fields and the data types of the output fields that you specify in the AS clause. If you do not specify an output field in this expression, the system uses the output fields that you specify in the AS clause.



Configure the AS clause to specify output fields for the CTE. The CTE returns result data based on the output fields that you specified in this clause.



After you define a CTE, you can reference the CTE in SQL statements.



## Examples



The following examples show how to use a CTE.



```

WITH T_CTE (i1_cte,i2_cte) AS (SELECT i1,i2 FROM t1)

SELECT * FROM T_CTE

```



The preceding sample code shows how to define and use a simple CTE. The SELECT statement returns the result set based on the output fields that you specified in the CTE.



```

WITH T_CTE (i1_cte,i2_cte) AS (SELECT i1,i2 FROM t1)

SELECT * FROM t2 JOIN T_CTE ON t2.i1 = T_CTE.i1_cte AND t2.i2 = T_CTE.i2_cte

```



In the preceding sample code, a CTE is used in the JOIN clause.



```

WITH T_CTE (i1_cte,i2_cte) AS (SELECT i1,i2 FROM t1)

SELECT * FROM t2 WHERE EXISTS (SELECT * FROM T_CTE WHERE t2.i1 = i1_cte AND t2.i2 = i2_cte)

```



In the preceding sample code, a CTE is used in the sub-query statement.



## FAQ

### Performence



Based on the information provided in the "Overview" section of this topic, a CTE is used to define a temporary result set. During the process of statement execution, the system does not execute the CTE to obtain a result set before the system executes the SELECT statement. In the actual execution process of a statement that includes a CTE, the system obtains the information that you defined in the CTE when the system generates an execution plan for the statement. Then, the system optimizes the SQL query based on the CTE and the SELECT statement in which you specified the CTE. The execution plan of a statement that includes a CTE is less complex than the execution plan of a statement that does not include a CTE. The query performance of these types of statements is also the same.
