## Overview



A summary index stores information about a document. Havenask can use IDs of documents to obtain the location at which the document information is stored. This way, Havenask can provide you with the stored summaries of the documents. The schema of a summary index is similar to the schema of a forward index. However, the features of the indexes are different.



## Sample code for configuring a summary index

```json

"summarys":

{

	"summary_fields":["id", "company_id", "subject", "cat_id"],

	"compress":false,

  "parameter" : {

      "compress_type" : "uniq|equal",    

      "file_compress" : "simple_compress1"

  }

}

```



- The summary_fields parameter specifies the fields that you want to store in your summary index. You can specify fields of all data types for this parameter.

- The compress parameter specifies whether to compress your summary index by using zlib. The valid values are true and false. The true value indicates that compression is required, and the false value indicates that  compression is not required. The default value is false.

- When you specify a field of the TIMESTAMP data type to create a summary index, the field is stored as an attribute. The time zone format specified by this field is used in the default time zone.

- You can use parameter to configure specific parameters. Havenask V3.9.1 and later support parameter. The following parameters are supported:

   - The compress_type parameter specifies the encoding method. For information about uniq and equal, see Forward index compression.

   - You can use the file_compress parameter to compress a file. You must specify the alias of a compressed file for the file_compress parameter after you specify the alias in the file named schema.json. The file_compress parameter provides better compression performance than the compress parameter. We recommend that you set the value of the compress parameter to false and configure the file_compress parameter.
