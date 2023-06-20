The biz.json file configures information about the table to be loaded and configurations of the cluster when the Query Result Searcher (QRS) worker is started. The following example shows how the biz.json file is configured:



```

{

    "dependency_table": [

        "in0"

    ],

    "cluster_config" : {

        "hash_mode" : {

            "hash_field" : "docid",

            "hash_function" : "HASH"

        },

        "query_config" : {

            "default_index" : "title",

            "default_operator" : "AND"

        },

        "table_name" : "in0"

    }

}

```

* dependency_table: specifies the table to be loaded.

* cluster_config: specifies cluster-related configurations.

   * hash_mode: specifies the hash mode of the cluster data, which mainly includes  the hash_field and hash_function fields. hash_field specifies hash fields and hash_functions specifies hash functions to be used.

* query_config: specifies the default parameter to execute a query.

   * default_index: specifies the default index that is queried when no index name is specified in the query.

   * default_operator: specifies the default operator used by the search result after tokenization, which can be AND or OR.

   * table_name: specifies the name of the default index table that is queried.


