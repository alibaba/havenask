<a name="Azuv3"></a>

## Formats of query results

The following formats of query results are supported: `string`, `json`, `full_json`, and `flatbuffers`. You can configure the format of query results by using one of the following methods:



- Specify the format in the configuration file. The format configuration globally takes effect after SQL in Havenask is started. By default, all queries return query results in the specified format.

- Specify the format in the kvpair clause of a query. The format configuration takes effect for the query. The query returns the results in the format that is specified in the kvpair clause regardless of the format specified in the configuration file.



<a name="qYXLm"></a>

### Specify the format of the query results in the kvpair clause of the query

In the `kvpair` clause of the query, specify the format of the query results by using `formatType:json` or `formatType:string`. For more information about how to use the `kvpair` clauses, see [kvpair clauses](https://github.com/alibaba/havenask/wiki/kvpair-clauses-en).

```

query=...&&kvpair=...;formatType:{json | string | full_json | flatbuffers};...

```



<a name="140Ah"></a>

### Query results returned in the string format

The query results in the string format are typeset. This ensures that the displayed results are user-friendly. In most cases, the query results in the string format are used for debugging. This helps with troubleshooting.



```

select nid, price, brand, size from phone

```



```

USE_TIME: 0.025, ROW_COUNT: 10



------------------------------- TABLE INFO ---------------------------

                 nid |               price |               brand |                size |

                   1 |                3599 |              Huawei |                 5.9 |

                   2 |                4388 |              Huawei |                 5.5 |

                   3 |                 899 |              Xiaomi |                   5 |

                   4 |                2999 |                OPPO |                 5.5 |

                   5 |                1299 |               Meizu |                 5.5 |

                   6 |                 169 |               Nokia |                 1.4 |

                   7 |                3599 |               Apple |                 4.7 |

                   8 |                5998 |               Apple |                 5.5 |

                   9 |                4298 |               Apple |                 4.7 |

                  10 |                5688 |             Samsung |                 5.6 |



------------------------------- TRACE INFO ---------------------------



------------------------------- SEARCH INFO ---------------------------

scanInfos { kernelName: "ScanKernel" nodeName: "0_0" tableName: "phone" hashKey: 2439343830 parallelNum: 1 totalOutputCount: 10 totalScanCount: 10 totalUseTime: 86 totalSeekTime: 29 totalEvaluateTime: 13 totalOu\

tputTime: 43 totalComputeTimes: 1 }

```





<a name="2XXyr"></a>

### Query results returned in the JSON format



The query results in the JSON format are used for service invocation. This facilitates program parsing. The results in the JSON format contain more information than the results in the string format. The following section describes each field that is included in the returned results.



```

select nid, price, brand, size from phone&&kvpair=formatType:json

```



```json

{

"error_info": // The error message returned.

  "{\"Error\": ERROR_NONE}",

"format_type":

  "json",

"row_count": // The number of rows in the results.

  10,

"search_info": // The information about the query, including operators such as scan and sort.

  "scanInfos { kernelName: \"ScanKernel\" nodeName: \"0_0\" tableName: \"phone\" hashKey: 2439343830 parallelNum: 1 totalOutputCount: 10 totalScanCount: 10 totalUseTime: 81 totalSeekTime: 28 totalEvaluateTime: 1\

1 totalOutputTime: 40 totalComputeTimes: 1 }",

"sql_result": // The returned results in the JSON or string format.

  "{\"column_name\":[\"nid\",\"price\",\"brand\",\"size\"],\"column_type\":[\"uint64\",\"double\",\"multi_char\",\"double\"],\"data\":[[1,3599,\"Huawei\",5.9],[2,4388,\"Huawei\",5.5],[3,899,\"Xiaomi\",5],[4,2999\

,\"OPPO\",5.5],[5,1299,\"Meizu\",5.5],[6,169,\"Nokia\",1.4],[7,3599,\"Apple\",4.7],[8,5998,\"Apple\",5.5],[9,4298,\"Apple\",4.7],[10,5688,\"Samsung\",5.6]]}",

"total_time": // The duration of the query. Unit: seconds.

  0.024,

"trace": // The trace information.

  [

  ]

}

```



<a name="8aQK2"></a>

### Query results returned in the FULL_JSON format

The results in the FULL_JSON format and the results in the JSON format use a similar structure. The only difference between the formats is the value of the sql_result field. In the JSON format, the value of the sql_result field is in the string format. In the FULL_JSON format, the value of the sql_result field is in the JSON format.

```json

{

	"total_time": 34.003,

	"covered_percent": 1,

	"row_count": 10 ,

	"format_type": "full_json",

	"search_info": {},

	"trace": [],

	"sql_result": {

		"data": [

			[

				232953260,

				"德克士"

			],

			[

				239745260,

				"叶氏兄弟"

			],

			[

				240084010,

				"菜老包"

			],

			[

				240082260,

				"周黑鸭"

			],

			[

				240086260,

				"绝味鸭脖"

			],

			[

				240108260,

				""

			],

			[

				239256390,

				"每一天生活超市"

			],

			[

				240079390,

				"美宜佳"

			],

			[

				265230260,

				""

			],

			[

				239313011,

				"大参林"

			]

		],

		"column_name": [

			"store_id",

			"brand_name"

		],

		"column_type": [

			"int64",

			"multi_char"

		]

	},

	"error_info": {

		"ErrorCode": 0,

		"Error": "ERROR_NONE",

		"Message": ""

	}

}

```



<a name="DM8yT"></a>

### sql_result description

```json

{

    "column_name":[ // The name of the column.

        "nid",

        "price",

        "brand",

        "size"

    ],

    "column_type":[ // The type of the column.

        "uint64",

        "double",

        "multi_char",

        "double"

    ],

    "data":[ // Each row of data.

        [

            1,

            3599,

            "Huawei",

            5.9

        ],

        [

            2,

            4388,

            "Huawei",

            5.5

        ],

        [

            3,

            899,

            "Xiaomi",

            5

        ],

        [

            4,

            2999,

            "OPPO",

            5.5

        ],

        [

            5,

            1299,

            "Meizu",

            5.5

        ],

        [

            6,

            169,

            "Nokia",

            1.4

        ],

        [

            7,

            3599,

            "Apple",

            4.7

        ],

        [

            8,

            5998,

            "Apple",

            5.5

        ],

        [

            9,

            4298,

            "Apple",

            4.7

        ],

        [

            10,

            5688,

            "Samsung",

            5.6

        ]

    ]

}

```



<a name="G8HkL"></a>

### searchInfo description



In the returned results, searchInfo is a complex field. You can analyze the query process to resolve issues and optimize performance based on the value of the searchInfo field. To include the searchInfo field in the returned results, add searchInfo:true to the kvpair clause.

```json

"search_info": {

	"exchangeInfos": [

		{

			"fillResultUseTime": 1276, // The period of time that is consumed by the exchange kernel to retrieve the results from the response.

			"hashKey": 4131140708,

			"kernelName": "ExchangeKernel", // The name of the exchange kernel.

			"nodeName": "1", // The name of the node to which the exchange kernel belongs.

			"poolSize": 472, // The size of the pool used by the worker to which the exchange kernel belongs in this query.

			"rowCount": 2, // The number of valid result rows that are returned after all columns are merged.

			"searcherUseTime": 7335, // The waiting time before a search request is initiated. Unit: microseconds.

			"totalAccessCount": 4 // The total number of columns that are accessed by the search request.

		}

	],

	"rpcInfos": [ // The details of the Remote Procedure Call (RPC) request. The number of elements that are contained in the field is equal to the number of columns that are returned for the field.

		{

			"beginTime": 1588131436272843, // The start time of calling the exchange kernel. Unit: microseconds.

			"rpcNodeInfos": [

				{

					"callUseTime": 5431, // The time consumed by the RPC node. Unit: microseconds.

					"isReturned": true, // Indicates whether the response is returned for the request.

					"netLatency": 664, // The network latency. Unit: microseconds.

					"rpcBegin": 1588131436272857, // The timestamp when the RPC began. Unit: microseconds.

					"rpcEnd": 1588131436278288, // The timestamp when the RPC ended. Unit: microseconds.

					"rpcUseTime": 5175, // The duration of the RPC. Unit: microseconds.

					"specStr": "11.1.130.2:20412" // The IP address and port used to call the server.

				},

				...

			],

				"totalRpcCount": 4, // The number of RPC requests that were sent.

				"useTime": 7335 // The total duration of the RPC.

			}

		],

		"runSqlTimeInfo": {

			"sqlPlan2GraphTime": 174, // The duration for converting the iquan plan to a navi graph. Unit: microseconds.

			"sqlPlanStartTime": 1588143112640920, // The timestamp when the request was sent to iquan to obtain a plan. Unit: microseconds.

			"sqlPlanTime": 10295, // The total duration between the time the request was sent to iquan and the time the plan was obtained. Unit: microseconds.

			"sqlRunGraphTime": 13407 // The total duration for executing the SQL graph. Unit: microseconds.

		},

		"scanInfos": [

			{

				"hashKey": 691673167,

				"kernelName": "ScanKernel", // The name of the kernel.

				"parallelNum": 2, // The degree of parallelism for scans. This field indicates the total number of scans executed in parallel.

				"parallelIndex": 1, // The sequence number of the scan kernel under the parallel logic.

				"tableName": "store", // The name of the table.

				"totalComputeTimes": 4, // The total number of times that batchScan was called.

				"totalEvaluateTime": 9, // The total time consumed to calculate the values for the fields. Unit: microseconds.

				"totalInitTime": 3758, // The total time spent in the init phase. Unit: microseconds.

				"totalOutputCount": 2, // The total number of output rows.

				"totalOutputTime": 264, // The total time consumed to build the output data, including adding or deleting data in the table. Unit: microseconds.

				"totalScanCount": 2, // The total number of scanned rows.

				"totalSeekTime": 2, // The total duration of the seek call. Unit: microseconds.

				"totalUseTime": 4217 // The total duration of calling the scan kernel. Unit: microseconds.

			}

		]

	}

```

<a name="4UYV5"></a>

##

<a name="RSX7i"></a>

### Query results returned in the flatbuffers format

The results in the flatbuffers format are the results of efficient serialization that is implemented based on FlatBuffers. The flatbuffers format is used when high performance is required. The results in the flatbuffers format need to be deserialized and parsed by the corresponding client. FlatBuffers is defined as follows:

```cpp

include "TwoDimTable.fbs";



namespace isearch.fbs;



table SqlErrorResult {

    partitionId: string (id:0);

    hostName: string (id:1);

    errorCode: uint (id:2);

    errorDescription: string (id:3);

}



table SqlResult {

    processTime: double (id:0);

    rowCount: uint32 (id:1);

    errorResult: SqlErrorResult (id:2);

    sqlTable: TwoDimTable (id:3);

    searchInfo: string (id:4);

    coveredPercent: double (id:5);

}



root_type SqlResult;

```

```

include "TsdbColumn.fbs";

namespace isearch.fbs;



// multi value

table MultiInt8   { value: [byte];   }

table MultiInt16  { value: [short];  }

table MultiInt32  { value: [int];    }

table MultiInt64  { value: [long];   }

table MultiUInt8  { value: [ubyte];  }

table MultiUInt16 { value: [ushort]; }

table MultiUInt32 { value: [uint];   }

table MultiUInt64 { value: [ulong];  }

table MultiFloat  { value: [float];  }

table MultiDouble { value: [double]; }

table MultiString { value: [string]; }



// column base storage

table Int8Column   { value: [byte];   }

table Int16Column  { value: [short];  }

table Int32Column  { value: [int];    }

table Int64Column  { value: [long];   }

table UInt8Column  { value: [ubyte];  }

table UInt16Column { value: [ushort]; }

table UInt32Column { value: [uint];   }

table UInt64Column { value: [ulong];  }

table FloatColumn  { value: [float];  }

table DoubleColumn { value: [double]; }

table StringColumn { value: [string]; }



table MultiInt8Column   { value: [MultiInt8];   }

table MultiUInt8Column  { value: [MultiUInt8];  }

table MultiInt16Column  { value: [MultiInt16];  }

table MultiUInt16Column { value: [MultiUInt16]; }

table MultiInt32Column  { value: [MultiInt32];  }

table MultiUInt32Column { value: [MultiUInt32]; }

table MultiInt64Column  { value: [MultiInt64];  }

table MultiUInt64Column { value: [MultiUInt64]; }

table MultiFloatColumn  { value: [MultiFloat];  }

table MultiDoubleColumn { value: [MultiDouble]; }

table MultiStringColumn { value: [MultiString]; }



// column type

union ColumnType {

      Int8Column,

      Int16Column,

      Int32Column,

      Int64Column,

      UInt8Column,

      UInt16Column,

      UInt32Column,

      UInt64Column,

      FloatColumn,

      DoubleColumn,

      StringColumn,

      MultiInt8Column,

      MultiInt16Column,

      MultiInt32Column,

      MultiInt64Column,

      MultiUInt8Column,

      MultiUInt16Column,

      MultiUInt32Column,

      MultiUInt64Column,

      MultiFloatColumn,

      MultiDoubleColumn,

      MultiStringColumn,

      TsdbDpsColumn,

}



table Column {

      name: string;

      value: ColumnType;

}



table TwoDimTable {

      rowCount: uint (id:0);

      columns: [Column] (id:1);

}

```

```cpp

namespace isearch.fbs;



struct TsdbDataPoint {

      ts: int64 (id:0);

      value: double (id:1);

}

table TsdbDataPointSeries { points: [TsdbDataPoint]; }

table TsdbDpsColumn { value : [TsdbDataPointSeries]; }

```

<a name="ywCo5"></a>

## Additional considerations

For the column_type field, the value that is prefixed with multi_ is of the multi-value type. The structure is a list. The value multi_char indicates values of the string type.


