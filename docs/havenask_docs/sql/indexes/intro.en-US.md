---
toc: content
---

# index overview

Each document consists of multiple fields. Each field contains a series of terms. You can build an index to speed up document retrievals. Indexes are categorized into the following types based on the direction of mappings:

### Inverted index
The inverted index stores the mappings from words to DocIDs, such as:
`Word -->(Doc1,Doc2,...,DocN)`

Inverted index is mainly used in retrieval, it can quickly locate the document corresponding to the user query keyword.

### forward index (attribute)
The forward index stores the mappings from DocIDs to fields, as shown in the following table.
`DocID-->(term1,term2,...termn)`

forward indexes can be divided into single-value and multi-value indexes. single-value attributes have a fixed length (excluding the string type), so they can be searched efficiently and can be updated. A multivalued attribute is a field that contains an indefinite number of data values. The length of a multivalued attribute is not fixed. This negatively affects query performance. You cannot update the data of a multivalued index.
After a document is retrieved, you can use a forward index to query the attributes of the document based on the document ID for statistics collection, sorting, and filtering.
Currently, the basic types of forward fields supported by the engine include INT8(8-bit signed number type), UINT8(8-bit unsigned number type), INT16(16-bit signed number type), UINT16(16-bit unsigned number type), INTEGER(32-bit signed number type), UINT32(32-bit unsigned number type), INT64(64-bit signed number type), UINT64(64-bit unsigned numeric type), FLOAT(32-bit floating-point number), DOUBLE(64-bit floating-point number), STRING (string type)
A multi-value attribute is a single-value attribute that may contain an indeterminate number of values in a field. The engine supports multi-value attributes (such as multi_int8 and multi_string) for the preceding single-value attributes.

### Summary
A summary is similar to an attribute. However, a summary stores multiple fields corresponding to a document and establishes a mapping. Therefore, the summary can be quickly located from the docid to the corresponding summary content.
A summary index is used to display search results. A summary index contains a large amount of data. For each query, you do not need to retrieve an excessive amount of data in summary indexes. Instead, you need to only obtain the search results from a document based on the summary index.
