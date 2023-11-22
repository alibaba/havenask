### SQL field types
Currently, SQL supports three field types:
* AtomicType: specifies the atomic type.
* ArrayType: specifies the type of multi-value fields that contain multiple values of an atomic type.
* MultiSetType: specifies  the type of child tables that contain values of multiple atomic types or array types.

The following atomic types are available:
* int8
* int16
* int32/integer
* int64/long
* float
* double
* string

For more information, see [Types in the system](https://help.aliyun.com/document_detail/398599.htm).

### Field type conversion
* Field types in the schema of Havenask V3 are automatically converted into SQL field types. Currently, SQL queries do not support data of the unsigned data type, and data of the unsigned type are automatically promoted to the signed type. In some scenarios, execution errors may occur due to type mismatch or overflow. We recommend that you do not specify a field of an unsigned type.
* You can use the CAST operator to convert the field type that is not supported by SQL into a valid field type.