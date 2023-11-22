---
toc: content
order: 1
---

# 数据类型
## SQL字段类型
目前SQL支持的字段类型主要分为三类：
* AtomicType: 基本原子类型
* ArrayType: 对应于多值字段，内部包含一个AtomicType
* MultiSetType: 对应于子表，内部包含多个AtomicType或ArrayType

AtomicType目前支持的类型:
* int8
* int16
* int32/integer
* int64/long
* float
* double
* string

更多说明可参考[系统中的各种类型](../../../indexes/fieldmapping)。

## 类型转换
* Ha3 Schema中的字段类型会自动转换成SQL类型；由于目前SQL不支持unsigned类型，转换时会自动提升成signed类型，在某些场景下会由于类型不匹配或溢出等原因导致执行错误，不建议使用；
* 如果存在类型不一致的场景，可以使用CAST操作符来转换类型；