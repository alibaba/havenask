---
toc: content
order: 400
---

# 查询结果解读
<a name="Azuv3"></a>
## 查询结果的返回形式
目前支持返回格式有四种： `string` 、 `json` 、`full_json`、`flatbuffers`，可以通过以下几种途径配置：

- 在配置文件设定：havenask SQL启动后全局生效，所有query默认以设定的形式返回查询结果
- 在query的kvpair子句中指定：仅对当前query生效，当前query以设定的形式返回查询结果（无视配置文件中的设定的形式）

<a name="qYXLm"></a>
### 在query的kvpair中设定返回值形式
在query的 `kvpair` 字段中，通过 `formatType:json` 或者 `formatType:string` 来控制查询结果的返回形式。更多的 `kvpair` 用法见[文档](../kvpair-clause.md)。
```
query=...&&kvpair=...;formatType:{json | string | full_json | flatbuffers};...
```

<a name="140Ah"></a>
### string形式结果解析
string形式的返回结果自带排版格式，结果直观，可读性较好。一般用于开发调试，方便用户排查问题。

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
### JSON形式结果解析

json形式的结果主要用于服务调用，方便程序解析，含有的信息量也更大，下面主要介绍一下各个返回字段的含义。

```
select nid, price, brand, size from phone&&kvpair=formatType:json
```

```json
{
"error_info": // 输出error信息
  "{\"Error\": ERROR_NONE}",
"format_type":
  "json",
"row_count": // 结果数
  10,
"search_info": // search 相关的一些信息，包括scan，sort等算子指标
  "scanInfos { kernelName: \"ScanKernel\" nodeName: \"0_0\" tableName: \"phone\" hashKey: 2439343830 parallelNum: 1 totalOutputCount: 10 totalScanCount: 10 totalUseTime: 81 totalSeekTime: 28 totalEvaluateTime: 1\
1 totalOutputTime: 40 totalComputeTimes: 1 }",
"sql_result": // 结果，返回json string，
  "{\"column_name\":[\"nid\",\"price\",\"brand\",\"size\"],\"column_type\":[\"uint64\",\"double\",\"multi_char\",\"double\"],\"data\":[[1,3599,\"Huawei\",5.9],[2,4388,\"Huawei\",5.5],[3,899,\"Xiaomi\",5],[4,2999\
,\"OPPO\",5.5],[5,1299,\"Meizu\",5.5],[6,169,\"Nokia\",1.4],[7,3599,\"Apple\",4.7],[8,5998,\"Apple\",5.5],[9,4298,\"Apple\",4.7],[10,5688,\"Samsung\",5.6]]}",
"total_time": // 耗时，单位s
  0.024,
"trace": // trace信息
  [
  ]
}
```

<a name="8aQK2"></a>
### FULL_JSON形式结果解析
full_json形式的结构与json形式比较相似，唯一的不同在于sql_result字段，json形式下是字符串形式，full_json下是直接用json表示的形式
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
### sql_result详解
```json
{
    "column_name":[ // 列名
        "nid",
        "price",
        "brand",
        "size"
    ],
    "column_type":[ // 列类型
        "uint64",
        "double",
        "multi_char",
        "double"
    ],
    "data":[ // 每行数据
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
### searchInfo详解

在返回结果中，searchInfo是一个较为复杂的字段，可以协助用户分析查询过程，以排查问题、优化性能需在kvpairs中加searchInfo:true
```json
"search_info": {
	"exchangeInfos": [
		{
			"fillResultUseTime": 1276, // exchange kernel从response结构中获取结果耗时
			"hashKey": 4131140708, 
			"kernelName": "ExchangeKernel", // exchange kernel名称
			"nodeName": "1", // exchange kernel所属节点名称
			"poolSize": 472, // exchange kernel所属worker在本次查询使用的pool大小
			"rowCount": 2, // 所有列返回合并后的有效结果行数
			"searcherUseTime": 7335, // 发起search请求的等待耗时，单位为us
			"totalAccessCount": 4 // 发起search请求的总列数
		}
	],
	"rpcInfos": [ // 发起的rpc请求详情，该项有多少列返回就有多少元素
		{
			"beginTime": 1588131436272843, // exchange kernel调用起始时间，单位为us
			"rpcNodeInfos": [
				{
					"callUseTime": 5431, // 该rpc node耗时，单位为us
					"isReturned": true, // 请求是否有返回
					"netLatency": 664, // 网络延时，单位为us
					"rpcBegin": 1588131436272857, // rpc调用开始时间戳，单位为us
					"rpcEnd": 1588131436278288, // rpc调用结束时间戳，单位为us
					"rpcUseTime": 5175, // rpc调用持续时间，单位为us
					"specStr": "11.1.130.2:20412" // 调用server的ip和端口
				},
				...
			],
				"totalRpcCount": 4, // 发出的rpc请求数
				"useTime": 7335 // rpc调用总时长
			}
		],
		"runSqlTimeInfo": {
			"sqlPlan2GraphTime": 174, // iquan plan转navi graph的耗时，单位为us
			"sqlPlanStartTime": 1588143112640920, // 向iquan发起plan的请求时间戳，单位为us
			"sqlPlanTime": 10295, // 向iquan发起请求到获取plan的总时长，单位为us
			"sqlRunGraphTime": 13407 // 执行sql图的总时长，单位为us
		},
		"scanInfos": [
			{
				"hashKey": 691673167,
				"kernelName": "ScanKernel", // kernel名称
				"parallelNum": 2, // scan并行度，全局有多少个
				"parallelIndex": 1, // 该scan kernel属于并行逻辑下的第几个，为0默认不显示
				"tableName": "store", // table名称
				"totalComputeTimes": 4, // batchScan被调用的总次数
				"totalEvaluateTime": 9, // 字段求值总耗时，单位为us
				"totalInitTime": 3758, // init阶段总耗时，单位为us
				"totalOutputCount": 2, // 输出的总行数
				"totalOutputTime": 264, // 构建输出数据的总耗时，包括增删表中数据，单位为us
				"totalScanCount": 2, // scan出的记录总数
				"totalSeekTime": 2, // seek调用的总耗时，单位为us
				"totalUseTime": 4217 // scan kernel调用的总时长，单位为us
			}
		]
	}
```
<a name="4UYV5"></a>
## 
<a name="RSX7i"></a>
### Flatbuffers形式
flatbuffers形式是基于flatbuffers实现的高效序列化结果，在对性能有高要求的场景下比较实用，该形式的返回结果需要对应的客户端反序列化解析，flatbuffers协议定义如下：
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
## 注意
column_type：multi_前缀为多值类型，结构为list，multi_char 为string类型

