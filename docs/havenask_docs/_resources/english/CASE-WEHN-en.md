CASE WHEN expressions are similar to if...else statements. If you set a WHEN condition to TRUE, the value of the THEN expression is used. If you set all WHEN conditions to FALSE, the value of the ELSE expression is used.





### Syntax

```

CASE

WHEN conditon_1 THEN expression_1

WHEN conditon_2 THEN expression_2

...

ELSE expression_n

END

```



### Examples

1. CASE WHEN is used as an output expression.



```

SELECT  

    CASE    

        WHEN warehouse_id=48 THEN warehouse_id

        WHEN warehouse_id=24 THEN id

        ELSE wave_status

    END AS aa

FROM    s_wmp_package_wave

WHERE   wave_status = 0

LIMIT   10;

```



2. CASE WHEN is used as a conditional expression.



```

SELECT * FROM

(

  SELECT  

  CASE    

    WHEN warehouse_id=48 THEN warehouse_id

    WHEN warehouse_id=24 THEN id

    ELSE wave_status

  END AS aa

  FROM    s_wmp_package_wave

  WHERE   wave_status = 0

) t

WHERE t.aa > 10

LIMIT   10;

SELECT *

FROM s_wmp_package_wave

WHERE CASE

        WHEN warehouse_id=48 THEN warehouse_id

  WHEN warehouse_id=24 THEN id

        ELSE buyer_id

END > 10

AND wave_status = 0

LIMIT   10;

```



### Note:

1. The values of all THEN and ELSE expressions must be of the same type.



2. CASE WHEN can be used only as an independent expression. CASE WHEN cannot be nested in other expressions or user-defined functions (UDFs). For example, the following CASE WHEN expression is not supported



```

SELECT *

FROM s_wmp_package_wave

WHERE CASE

        WHEN warehouse_id=48 THEN warehouse_id

  WHEN warehouse_id=24 THEN id

        ELSE buyer_id

END + wave_status> 10

LIMIT   10;

```



3. CASE WHEN cannot be used for multi-value fields.



4. CASE WHEN must include ELSE expressions.