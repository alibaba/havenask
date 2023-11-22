---
toc: content
order: 1
---

# Data type
## SQL field type
The data types that are supported by SQL are divided into the following three categories:
* Atomic type: a basic data type.
* Array type: the type of a multivalued field that contains elements of an atomic type.
* Multiset type: the type of a child table that contains values of multiple atomic types or array types.

Currently supported types of AtomicType:
* int8
* int16
* int32/integer
* int64/long
* float
* double
* string

For more information about the data types supported by the system, see [Built-in field types](../../../indexes/fieldmapping).

## convert data types
* In OpenSearch Vector Search Edition, the data types in a schema are automatically converted to the data types supported by SQL. The unsigned types in the schema are automatically converted to the signed types because SQL does not support unsigned types. Therefore, execution errors may occur due to type match failures or overflows. We recommend that you do not use the type conversion feature.
* If data types are inconsistent, you can use the CAST operator to convert data types.