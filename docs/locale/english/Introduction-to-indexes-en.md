Each document consists of multiple fields. Each field contains a series of terms. You can build an index to speed up document retrievals. Indexes are categorized into the following types based on the direction of mappings:



## Inverted indexes

Inverted indexes store mappings from terms to document IDs in the following format:

`term -> (Doc1,Doc2,...,DocN)`



Inverted indexes are used for retrievals to help users identify the documents that contain specific search keywords.



## Attribute indexes

Attribute indexes store mappings from document IDs to fields in the following format:

`DocID-->(term1,term2,...termn)`



Attribute indexes include single-value attributes and multi-value attributes. A single-value attribute that excludes the STRING type has a fixed length. This improves query performance. You can update the data of a single-value attribute. A multi-value attribute is a field that contains an indefinite number of data values. The length of a multi-value attribute is not fixed. This negatively affects query performance. You cannot update the data of a multi-value index.

After a document is retrieved, you can use a forward index to query the attributes of the document based on the document ID for statistics collection, sorting, and filtering.

In Havenask, the following data types are supported for the fields in single-value attribute indexes: INT8, UINT8, INT16, UINT16, INTEGER, UINT32, INT64, UINT64, FLOAT, DOUBLE, and STRING.

A multi-value forward index can be regarded as a single-value forward index that is created based on a field that contains an indefinite number of values. Therefore, multi-value attribute indexes support the same data types as single-value attribute indexes.



## Summary indexes

Summary indexes store mappings from document IDs to summaries. The format of a summary index is similar to that of a forward index. However, in a summary index, a document ID is mapped to a collection of fields. You can use a summary index to identify the summary that corresponds to a document ID in a short period of time.

Summary indexes are used to retrieve results that contain the values of the fields that you want to display. In most cases, the size of a summary is large. Summary indexes are not suitable for searches in which a large amount of summary content needs to be retrieved. Summary content can be retrieved only for documents that contain the values of the fields that you want to display.

Havenask provides a compression mechanism for summary indexes. If you enable compression for a summary index in the schema, Havenask uses zlib to compress the summary index and then stores the compressed summary index. When Havenask reads data from the summary index, the search engine decompresses the compressed summary index and then returns the retrieved results to the user.