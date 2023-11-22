---
toc: content
---

# 索引介绍

每个Document都是由多个field组成，每个field中包含一系列的词语，构建索引的目的是为了加快检索的速度，根据映射关系方向的不同，索引可以分为倒排、正排、摘要三种类型。一份标准的havenask索引schema可以参见[schema配置](../petool/config/schemaconfig.md)

### 倒排索引（index）
倒排索引存储了从单词到DocID的映射关系，形如:
`词-->(Doc1,Doc2,...,DocN)`

倒排索引主要用在检索中，它能快速的定位用户查询到关键字对应的Document。 

### 正排索引（attribute）
正排索引存储从DocID到field的映射关系，形如:
`DocID-->(term1,term2,...termn)`

正排索引分单值和多值两种，单值attribute由于长度是固定的（不包括string类型），因此查找效率高，而且可以支持更新。多值attribute表示某个field中有多个数据（数量不固定），由于长度不确定，因此查找效率相较与单值更慢，而且不能支持更新。
正排索引主要是在查询到了某个Document后，根据docid值能快速获取到其attribute用来统计、排序、过滤中。
目前引擎支持的正排字段基本类型包括：INT8(8位有符号数字类型), UINT8(8位无符号数字类型), INT16(16位有符号数字类型), UINT16(16位无符号数字类型), INTEGER(32位有符号数字类型), UINT32(32位无符号数字类型), INT64(64位有符号数字类型), UINT64(64位无符号数字类型), FLOAT(32位浮点数), DOUBLE(64位浮点数), STRING(字符串类型)
多值的attribute只是一个field中可能出现数量不确定的单值attribute，引擎对上述的单值类型attribute都支持对应的多值attribute（例如multi_int8，multi_string）。 

### 摘要（summary）
summary的存储形式与attribute类似，但是summary是将一个Document对应的多个field存储在一起，并且建立映射，所以能很快从docid定位到对应的summary内容。
summary主要是用于结果的展示，一般而言summary的内容都比较大，对于每次查询而言不适合取过多的summary，只有最终需要展示结果的Document会取到对应的summary。
