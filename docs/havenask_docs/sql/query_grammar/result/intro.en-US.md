---
toc: content
order: 400
---

# interpret query results
<a name="Azuv3"></a>
## Formats of query results
The following formats of query results are supported: `string`, `json`, `full_json`, and `flatbuffers`. You can configure the format of query results by using one of the following methods:

- In the configuration file, set the havenask SQL statement to take effect globally after it is started. By default, all queries return query results in the specified format.
- Specify the format in the kvpair clause of a query. The format configuration takes effect for the query. The query returns the results in the format that is specified in the kvpair clause regardless of the format specified in the configuration file.

<a name="qYXLm"></a>
### Set the return value format in kvpair of a query
In the `kvpair` field of a query, you can use `formatType:json` or `formatType:string` to control how the query results are returned. See the [documentation](../kvpair-clause.md) for more `kvpair` usage.
```
query=...&&kvpair=...;formatType:{json | string | full_json | flatbuffers};...
```

<a name="140Ah"></a>
### Result parsing in string form
The query results that are returned in the string format are typeset. This ensures that the results are easy to view and readable. In most cases, the query results in the string format are used for debugging. This helps with troubleshooting.

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
### Result parsing in JSON format

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
"row_count": // The number of rows of results.
  10,
"search_info": // The information about the query, including operators such as scan and sort.
  "scanInfos { kernelName: \"ScanKernel\" nodeName: \"0_0\" tableName: \"phone\" hashKey: 2439343830 parallelNum: 1 totalOutputCount: 10 totalScanCount: 10 totalUseTime: 81 totalSeekTime: 28 totalEvaluateTime: 1\
1 totalOutputTime: 40 totalComputeTimes: 1 }",
"sql_result": // The returned results in the JSON or string format.
  "{\"column_name\":[\"nid\",\"price\",\"brand\",\"size\"],\"column_type\":[\"uint64\",\"double\",\"multi_char\",\"double\"],\"data\":[[1,3599,\"Huawei\",5.9],[2,4388,\"Huawei\",5.5],[3,899,\"Xiaomi\",5],[4,2999\
,\"OPPO\",5.5],[5,1299,\"Meizu\",5.5],[6,169,\"Nokia\",1.4],[7,3599,\"Apple\",4.7],[8,5998,\"Apple\",5.5],[9,4298,\"Apple\",4.7],[10,5688,\"Samsung\",5.6]]}",
"total_time": // The total time that is spent. Unit: seconds.
  0.024,
"trace": // The trace information.
  [
  ]
}
```

<a name="8aQK2"></a>
### Result parsing in the form of FULL_JSON
The results in the full_json format and the results in the JSON format use a similar structure. The only difference between the formats is the value of the sql_result field. In the JSON format, the value of the sql_result field is in the string format. In the full_json format, the value of the sql_result field is in the JSON format.
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
				"Dicos"
			],
			[
				239745260,
				"YesBrother"
			],
			[
				240084010,
				"Cailaobao"
			],
			[
				240082260,
				"ZHOU HEI YA"
			],
			[
				240086260,
				"Juewei Food"
			],
			[
				240108260,
				""
			],
			[
				239256390,
				"Everyday Chain"
			],
			[
				240079390,
				"Meiyijia"
			],
			[
				265230260,
				""
			],
			[
				239313011,
				"DaShenLin"
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
### sql_result
```json
{
    "column_name":[ // The name of a column.
        "nid",
        "price",
        "brand",
        "size"
    ],
    "column_type":[ // The type of a column.
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
### Detailed explanation of searchInfo

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
			"searcherUseTime": 7335, // The waiting time for initiating a search request. Unit: microseconds.
			"totalAccessCount": 4 // The total number of columns that are accessed by a search request.
		}
	],
	"rpcInfos": [ // The details of a Remote Procedure Call (RPC) request. The number of elements that are contained in the field is equal to the number of columns that are returned for the field.
		{
			"beginTime": 1588131436272843, // The start time of calling the exchange kernel. Unit: microseconds.
			"rpcNodeInfos": [
				{
					"callUseTime": 5431, // The time consumed by the RPC node. Unit: microseconds.
					"isReturned": true, // Specifies whether a response is returned for the request.
					"netLatency": 664, // The network latency. Unit: microseconds.
					"rpcBegin": 1588131436272857, // The timestamp when the RPC begins. Unit: microseconds.
					"rpcEnd": 1588131436278288, // The timestamp when the RPC ends. Unit: microseconds.
					"rpcUseTime": 5175, // The duration of the RPC. Unit: microseconds.
					"specStr": "11.1.130.2:20412" // The IP address and port used to call the server.
				},
				...
			],
				"totalRpcCount": 4, // The number of RPC requests that are sent.
				"useTime": 7335 // The total time of the RPC.
			}
		],
		"runSqlTimeInfo": {
			"sqlPlan2GraphTime": 174, // The duration for converting the iquan plan to a navi graph. Unit: microseconds.
			"sqlPlanStartTime": 1588143112640920, // The timestamp when the request is sent to iquan to obtain a plan. Unit: microseconds.
			"sqlPlanTime": 10295, // The total duration between the time you send a request to iquan and the time you obtain the plan. Unit: microseconds.
			"sqlRunGraphTime": 13407 // The total duration for executing the SQL graph. Unit: microseconds.
		},
		"scanInfos": [
			{
				"hashKey": 691673167,
				"kernelName": "ScanKernel", // The name of the kernel.
				"parallelNum": 2, // The degree of parallelism for scans. This field indicates the number of scans in parallel in the global.
				"parallelIndex": 1, // The sequence number of the scan kernel under the parallel logic. If the value is 0, the scan kernel is not displayed under the parallel logic by default.
				"tableName": "store", // The name of a table.
				"totalComputeTimes": 4, // The total number of times that batchScan is called.
				"totalEvaluateTime": 9, // The total time consumed to evaluate a field. Unit: microseconds.
				"totalInitTime": 3758, // The total time spent in the initialization phase. Unit: microseconds.
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
### Flatbuffers Form
The flatbuffers format is an efficient serialization result based on flatbuffers. It is practical in scenarios with high performance requirements. The returned result of this format needs to be deserialized and parsed by the corresponding client. The flatbuffers protocol is defined as follows:
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
For the column_type field, the value that is prefixed with multi_ is of the multi-value type. The structure is a list. The value multi_char indicates a value of the string type.

