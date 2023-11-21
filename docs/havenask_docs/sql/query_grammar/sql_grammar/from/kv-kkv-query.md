---
toc: content
order: 103
---

# KV、KKV查询
## 描述
KV、KKV查询用于支持havenask中的KV表和KKV表，Iquan管理KV表和KKV表的注册信息。

KV、KKV查询时必须包含PK条件（详情见下文“使用限制”）。


## 示例
### 1. KV表
* Note：示例中KV表category的PK字段为cat_id。
```
SELECT cat_id, category_name FROM category where cat_id=2;

SELECT cat_id, category_name FROM category where cat_id in (2, 3);
```

### 2. KKV表

* Note：示例中KKV表company的PK字段为company_id。

```

SELECT company_id, company_name FROM company where company_id in (1,2);

SELECT company_id, company_name FROM company where company_id = 1 OR company_id = 2;
```

### 3. 使用限制
* KV、KKV单表查询， where 子句后必须含有PK字段的等值查询条件。
* 如果顶层查询条件由AND连接，必须保证至少有一个子条件是PK字段的等值查询条件。

```
SELECT company_id, company_name 
FROM company 
WHERE company_id IN (1,2) AND company_name <> 'HEHE';
```

* 如果顶层查询条件由OR连接，必须保证所有的子条件都有PK字段的等值查询条件。

```
SELECT company_id, company_name 
FROM company 
WHERE 
        (company_id = 1 AND company_name <> 'HEHE') 
  OR 
  (company_id IN (3,4,5) AND company_name = 'HEHE') 
;
```

* KV/KKV表与其他表关联时，只能支持LookupJoin，并且KV/KKV表必须为build表；