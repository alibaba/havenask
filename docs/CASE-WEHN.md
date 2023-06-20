功能类似于if...else if...else语句。如果某个when条件为TRUE，则取对应的then表达式的值；如果所有when条件都为FALSE，则取else表达式的值。


## 语法格式​
```
CASE 
WHEN conditon_1 THEN expression_1
WHEN conditon_2 THEN expression_2
……
ELSE expression_n
END
```

## 示例
1. case when 作为输出表达式

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

2. case when 作为条件表达式

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

## 注意
1. 所有THEN、ELSE表达式的值的类型必须一致。

2. 目前只支持CASE WHEN作为一个独立的表达式使用，而不能嵌套于其它表达式或UDF中。如下面表达式暂时不支持：

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

3. CASE WHEN不能用于多值字段。

4. 一定要有ELSE分支。