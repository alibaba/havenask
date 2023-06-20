## Overview



A forward index is an index that stores the values of a specific field (attribute) in a document and is used for filtering, counting, sorting, or scoring. The forward index is also known as an attribute index or a profile index. “Forward” indicates the mapping from documents to fields in the documents.

## Attribute types of attribute indexes



The engine supports the following attribute types:



| Attribute type | Description | Single-value | Multi-value |
| --- | --- | --- | --- |
| INT8 | Stores 8-bit signed integers. | id=-27 | id=-27]26]33 |
| UINT8 | Stores 8-bit unsigned integers. | id=56 | id=27]222]32 |
| INT16 | Stores 16-bit signed integers. | id=-2724 | id=-1217^]236 |
| UINT16 | Stores 16-bit unsigned integers. | id=65531 | id=-65531]22236]0^]1 |
| INTEGER | Stores 32-bit signed integers. | id=-655312 | id=-2714442]2344126]33441 |
| UINT32 | Stores 32-bit unsigned integers. | id=65537 | id=4011341512]26]33 |
| INT64 | Stores 64-bit signed integers. | id=-551533527 | id=-2416664447]236]133 |
| UINT64 | Stores 64-bit unsigned integers. | id=23545114533527 | id=12416664447]121436]2 |
| FLOAT | Stores single-precision 32-bit floating points. | id=3.14 | id=3.25]3.50]3.75 |
| DOUBLE | Stores double-precision 64-bit floating points. | id=3.1415926 | id=-3.1415926]26.1444]55.1441 |
| STRING | Stores strings. | id=abc | id=abc]def]dgg^]dd |


