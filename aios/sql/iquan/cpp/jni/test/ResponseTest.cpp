#include "autil/Log.h"
#include "autil/legacy/test/JsonTestUtil.h"
#include "iquan/common/Utils.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "unittest/unittest.h"

using namespace testing;

namespace iquan {

class ResponseTest : public TESTBASE {
public:
    static std::shared_ptr<autil::legacy::RapidDocument> parseDocument(const std::string &str);
    static std::string formatJsonStr(const std::string &str);

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(iquan, ResponseTest);

std::shared_ptr<autil::legacy::RapidDocument> ResponseTest::parseDocument(const std::string &str) {
    std::shared_ptr<autil::legacy::RapidDocument> documentPtr(new autil::legacy::RapidDocument);
    std::string newStr = str + IQUAN_DYNAMIC_PARAMS_SIMD_PADDING;
    documentPtr->Parse(newStr.c_str());
    return documentPtr;
}

std::string ResponseTest::formatJsonStr(const std::string &str) {
    autil::legacy::json::JsonMap jsonMap;
    Utils::fromJson(jsonMap, str);
    std::string newStr;
    Utils::toJson(jsonMap, newStr);
    return newStr;
}

TEST_F(ResponseTest, testSimple) {
    std::string planStr = R"json(
{
  "error_code" : 0,
  "error_message" : "",
  "result": {
    "exec_params": {},
    "rel_plan": [
      {
        "attrs": {
          "catalog_name": "test",
          "db_name": "phone",
          "hash_fields": [
            "nid"
          ],
          "hash_type": "HASH",
          "limit": 2147483647,
          "output_fields": [
            "$nid",
            "$brand",
            "$price",
            "$size"
          ],
          "output_fields_type": [
            "BIGINT",
            "VARCHAR",
            "DOUBLE",
            "DOUBLE"
          ],
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$nid"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 5
          },
          "table_name": "phone",
          "table_type": "normal",
          "use_nest_table": false,
          "used_fields": [
            "$size",
            "$nid",
            "$brand",
            "$price"
          ]
        },
        "id": 0,
        "inputs": {
          "input": []
        },
        "op_name": "TableScanOp"
      },
      {
        "attrs": {
          "directions": [
            "ASC"
          ],
          "limit": 2,
          "offset": 0,
          "order_fields": [
            "$nid"
          ]
        },
        "id": 1,
        "inputs": {
          "input": [
            0
          ]
        },
        "op_name": "SortOp"
      },
      {
        "attrs": {
          "catalog_name": "test",
          "db_name": "phone",
          "distribution": {
            "type": "SINGLETON"
          },
          "source_id": "",
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$nid"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 5
          },
          "table_name": "phone",
          "table_type": "normal"
        },
        "id": 2,
        "inputs": {
          "input": [
            1
          ]
        },
        "op_name": "ExchangeOp"
      },
      {
        "attrs": {
          "directions": [
            "ASC"
          ],
          "limit": 2,
          "offset": 0,
          "order_fields": [
            "$nid"
          ]
        },
        "id": 3,
        "inputs": {
          "input": [
            2
          ]
        },
        "op_name": "SortOp"
      },
      {
        "attrs": {
          "type": "api"
        },
        "id": 4,
        "inputs": {
          "input": [
            3
          ]
        },
        "op_name": "SinkOp"
      }
    ],
    "rel_plan_version": "plan_version_0.0.1"
  }
}
)json";

    std::string expectPlanStr = formatJsonStr(planStr);

    std::shared_ptr<autil::legacy::RapidDocument> documentPtr = parseDocument(planStr);
    IquanDqlResponse response;
    Status status = Utils::fromRapidValue(response, documentPtr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    DynamicParams dynamicParams;
    dynamicParams._array.push_back({});
    ASSERT_EQ(1, dynamicParams._array.size());
    ASSERT_EQ(0, dynamicParams._array[0].size());
    IquanDqlResponseWrapper responseWrapper;
    responseWrapper.convert(response, dynamicParams);
    std::string actualPlanStr;
    status = Utils::toJson(responseWrapper, actualPlanStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanStr, actualPlanStr));
}

TEST_F(ResponseTest, testOptimizeInfos) {
    std::string planStr = R"json({
  "error_code" : 0,
  "error_message" : "",
  "result": {
    "rel_plan_version" : "xxx",
    "rel_plan" : [],
    "exec_params" : {
    },
    "optimize_infos" : {
        "0": [
            {
                "optimize_info" : {
                    "op": "xxx",
                    "type": "UDF",
                    "params": ["abc"]
                },
                "replace_key" : ["a"],
                "replace_type" : ["int"]
            }
        ]
    }
}
}
)json";
    std::shared_ptr<autil::legacy::RapidDocument> documentPtr = parseDocument(planStr);
    IquanDqlResponse response;
    Status status = Utils::fromRapidValue(response, documentPtr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();
    auto &sqlPlan = response.sqlPlan;
    ASSERT_EQ(1, sqlPlan.optimizeInfosMap.size());
    auto &optimizeInfos = sqlPlan.optimizeInfosMap["0"];
    ASSERT_EQ("a", optimizeInfos[0].key[0]);
    ASSERT_EQ("int", optimizeInfos[0].type[0]);
    ASSERT_EQ("xxx", optimizeInfos[0].optimizeInfo.op);
    ASSERT_EQ("UDF", optimizeInfos[0].optimizeInfo.type);
    ASSERT_EQ(1, optimizeInfos[0].optimizeInfo.params.size());
}

TEST_F(ResponseTest, testSimpleDynamicParams) {
    std::string planStr = R"json(
{
  "error_code" : 0,
  "error_message" : "",
  "result": {
    "exec_params": {},
    "rel_plan": [
      {
        "attrs": {
          "catalog_name": "default",
          "condition": {
            "op": "OR",
            "params": [
              {
                "op": "AND",
                "params": [
                  {
                    "op": ">",
                    "params": [
                      "$i2",
                      "[dynamic_params[[?0#int32]]dynamic_params]"
                    ],
                    "type": "OTHER"
                  },
                  {
                    "op": "<",
                    "params": [
                      "$d3",
                      "[dynamic_params[[?1#float]]dynamic_params]"
                    ],
                    "type": "OTHER"
                  }
                ],
                "type": "OTHER"
              },
              {
                "op": "=",
                "params": [
                  "$s5",
                  "[dynamic_params[[?2#string]]dynamic_params]"
                ],
                "type": "OTHER"
              }
            ],
            "type": "OTHER"
          },
          "db_name": "db1",
          "hash_fields": [
            "i1",
            "i2"
          ],
          "hash_type": "HASH",
          "index_infos": {
            "$i2": {
              "name": "i2",
              "type": "number"
            }
          },
          "limit": 100,
          "output_fields": [
            "$i1"
          ],
          "output_fields_type": [
            "INTEGER"
          ],
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$i1",
                "$i2"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 64
          },
          "table_name": "t1",
          "table_type": "normal",
          "use_nest_table": false,
          "used_fields": [
            "$d3",
            "$s5",
            "$i1",
            "$i2"
          ]
        },
        "id": 0,
        "inputs": {
          "input": []
        },
        "op_name": "TableScanOp"
      },
      {
        "attrs": {
          "catalog_name": "default",
          "db_name": "db1",
          "distribution": {
            "type": "SINGLETON"
          },
          "source_id": "",
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$i1",
                "$i2"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 64
          },
          "table_name": "t1",
          "table_type": "normal"
        },
        "id": 1,
        "inputs": {
          "input": [
            0
          ]
        },
        "op_name": "ExchangeOp"
      },
      {
        "attrs": {
          "limit": 100,
          "offset": 0
        },
        "id": 2,
        "inputs": {
          "input": [
            1
          ]
        },
        "op_name": "LimitOp"
      },
      {
        "attrs": {
          "type": "api"
        },
        "id": 3,
        "inputs": {
          "input": [
            2
          ]
        },
        "op_name": "SinkOp"
      }
    ],
    "rel_plan_version": "plan_version_0.0.1"
  }
})json";

    std::string expectPlanStr = R"json(
{
  "error_code" : 0,
  "error_message" : "",
  "result": {
    "exec_params": {},
    "rel_plan": [
      {
        "attrs": {
          "catalog_name": "default",
          "condition": {
            "op": "OR",
            "params": [
              {
                "op": "AND",
                "params": [
                  {
                    "op": ">",
                    "params": [
                      "$i2",
                      200
                    ],
                    "type": "OTHER"
                  },
                  {
                    "op": "<",
                    "params": [
                      "$d3",
                      520.67
                    ],
                    "type": "OTHER"
                  }
                ],
                "type": "OTHER"
              },
              {
                "op": "=",
                "params": [
                  "$s5",
                  "str1"
                ],
                "type": "OTHER"
              }
            ],
            "type": "OTHER"
          },
          "db_name": "db1",
          "hash_fields": [
            "i1",
            "i2"
          ],
          "hash_type": "HASH",
          "index_infos": {
            "$i2": {
              "name": "i2",
              "type": "number"
            }
          },
          "limit": 100,
          "output_fields": [
            "$i1"
          ],
          "output_fields_type": [
            "INTEGER"
          ],
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$i1",
                "$i2"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 64
          },
          "table_name": "t1",
          "table_type": "normal",
          "use_nest_table": false,
          "used_fields": [
            "$d3",
            "$s5",
            "$i1",
            "$i2"
          ]
        },
        "id": 0,
        "inputs": {
          "input": []
        },
        "op_name": "TableScanOp"
      },
      {
        "attrs": {
          "catalog_name": "default",
          "db_name": "db1",
          "distribution": {
            "type": "SINGLETON"
          },
          "source_id": "",
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$i1",
                "$i2"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 64
          },
          "table_name": "t1",
          "table_type": "normal"
        },
        "id": 1,
        "inputs": {
          "input": [
            0
          ]
        },
        "op_name": "ExchangeOp"
      },
      {
        "attrs": {
          "limit": 100,
          "offset": 0
        },
        "id": 2,
        "inputs": {
          "input": [
            1
          ]
        },
        "op_name": "LimitOp"
      },
      {
        "attrs": {
          "type": "api"
        },
        "id": 3,
        "inputs": {
          "input": [
            2
          ]
        },
        "op_name": "SinkOp"
      }
    ],
    "rel_plan_version": "plan_version_0.0.1"
  }
})json";

    expectPlanStr = formatJsonStr(expectPlanStr);

    std::shared_ptr<autil::legacy::RapidDocument> documentPtr = parseDocument(planStr);
    IquanDqlResponse response;
    Status status = Utils::fromRapidValue(response, documentPtr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    std::vector<autil::legacy::Any> dynamicParam;
    dynamicParam.push_back(autil::legacy::Any(200));
    dynamicParam.push_back(autil::legacy::Any(520.67));
    dynamicParam.push_back(autil::legacy::Any(std::string("str1")));
    DynamicParams dynamicParams;
    dynamicParams._array.push_back(dynamicParam);
    IquanDqlResponseWrapper responseWrapper;
    responseWrapper.convert(response, dynamicParams);
    std::string actualPlanStr;
    status = Utils::toJson(responseWrapper, actualPlanStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanStr, actualPlanStr));
}

TEST_F(ResponseTest, testReplaceHintParams) {
    std::string planStr = R"json(
{
  "error_code" : 0,
  "error_message" : "",
  "result": {
    "exec_params": {},
    "rel_plan": [
      {
        "attrs": {
          "catalog_name": "default",
          "db_name": "db1",
          "distribution": {
            "type": "SINGLETON"
          },
          "source_id": "",
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$i1",
                "$i2"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 64,
            "assigned_partition_ids": "[hint_params[partition]hint_params]"
          },
          "table_name": "t1",
          "table_type": "normal"
        },
        "id": 1,
        "inputs": {
          "input": [
            0
          ]
        },
        "op_name": "ExchangeOp"
      }
    ],
    "rel_plan_version": "plan_version_0.0.1"
  }
})json";

    std::string expectPlanStr = R"json(
{
  "error_code" : 0,
  "error_message" : "",
  "result": {
    "exec_params": {},
    "rel_plan": [
      {
        "attrs": {
          "catalog_name": "default",
          "db_name": "db1",
          "distribution": {
            "type": "SINGLETON"
          },
          "source_id": "",
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$i1",
                "$i2"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 64,
            "assigned_partition_ids": "1,2"
          },
          "table_name": "t1",
          "table_type": "normal"
        },
        "id": 1,
        "inputs": {
          "input": [
            0
          ]
        },
        "op_name": "ExchangeOp"
      }
    ],
    "rel_plan_version": "plan_version_0.0.1"
  }
})json";

    expectPlanStr = formatJsonStr(expectPlanStr);

    std::shared_ptr<autil::legacy::RapidDocument> documentPtr = parseDocument(planStr);
    IquanDqlResponse response;
    Status status = Utils::fromRapidValue(response, documentPtr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    DynamicParams dynamicParams;
    dynamicParams._hint["partition"] = "1,2";
    IquanDqlResponseWrapper responseWrapper;
    responseWrapper.convert(response, dynamicParams);
    std::string actualPlanStr;
    status = Utils::toJson(responseWrapper, actualPlanStr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    ASSERT_NO_FATAL_FAILURE(
        autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanStr, actualPlanStr));
}

TEST_F(ResponseTest, testNonValidPlanOp) {
    // not object
    {
        std::string planOpStr = R"json(
      ["id"]
)json";

        std::shared_ptr<autil::legacy::RapidDocument> documentPtr = parseDocument(planOpStr);
        PlanOp planOp;
        Status status = Utils::fromRapidValue(planOp, documentPtr);
        ASSERT_FALSE(status.ok());
    }

    // no attrs
    {
        std::string planOpStr = R"json(
      {
        "id": 1
      }
)json";

        std::shared_ptr<autil::legacy::RapidDocument> documentPtr = parseDocument(planOpStr);
        PlanOp planOp;
        Status status = Utils::fromRapidValue(planOp, documentPtr);
        ASSERT_FALSE(status.ok());
    }

    // attrs is not object
    {
        std::string planOpStr = R"json(
      {
        "id": 1,
        "attrs" : "abc"
      }
)json";

        std::shared_ptr<autil::legacy::RapidDocument> documentPtr = parseDocument(planOpStr);
        PlanOp planOp;
        Status status = Utils::fromRapidValue(planOp, documentPtr);
        ASSERT_FALSE(status.ok());
    }
}

TEST_F(ResponseTest, testPlanOpPatchable) {
    std::string planOpStr = R"json(
      {
        "attrs": {
          "catalog_name": "default",
          "db_name": "db1",
          "distribution": {
            "type": "SINGLETON"
          },
          "source_id": "",
          "table_distribution": {
            "hash_mode": {
              "hash_fields": [
                "$i1",
                "$i2"
              ],
              "hash_function": "HASH"
            },
            "partition_cnt": 64
          },
          "table_name": "t1",
          "table_type": "normal"
        },
        "id": 1,
        "inputs": {
          "input": [
            0
          ]
        },
        "op_name": "ExchangeOp"
      }
)json";

    std::shared_ptr<autil::legacy::RapidDocument> documentPtr = parseDocument(planOpStr);
    PlanOp planOp;
    Status status = Utils::fromRapidValue(planOp, documentPtr);
    ASSERT_TRUE(status.ok()) << status.errorMessage();

    std::vector<std::vector<autil::legacy::Any>> dynamicParams;
    dynamicParams.push_back({});
    ASSERT_EQ(1, dynamicParams.size());
    ASSERT_EQ(0, dynamicParams[0].size());

    // patch string attr
    {
        std::string expectPlanOpStr = R"json({
"attrs":
  {
  "catalog_name":
    "default",
  "db_name":
    "db1",
  "distribution":
    {
    "type":
      "SINGLETON"
    },
  "source_id":
    "",
  "str_key":
    "string_value",
  "table_distribution":
    {
    "hash_mode":
      {
      "hash_fields":
        [
          "$i1",
          "$i2"
        ],
      "hash_function":
        "HASH"
      },
    "partition_cnt":
      64
    },
  "table_name":
    "t1",
  "table_type":
    "logical"
  },
"id":
  1,
"inputs":
  {
  "input":
    [
      0
    ]
  },
"op_name":
  "ExchangeOp"
})json";

        expectPlanOpStr = formatJsonStr(expectPlanOpStr);

        planOp.patchStrAttrs["str_key"] = std::string("string_value");
        planOp.patchStrAttrs["table_type"] = std::string("logical");

        PlanOpWrapper planOpWrapper;
        DynamicParams dynamicParamss;
        dynamicParamss._array = dynamicParams;
        DynamicParams innerDynamicParams;
        ASSERT_TRUE(planOpWrapper.convert(planOp, dynamicParamss, innerDynamicParams));
        std::string actualPlanOpStr;
        status = Utils::toJson(planOpWrapper, actualPlanOpStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanOpStr, actualPlanOpStr));
    }

    // patch double attr
    {
        std::string expectPlanOpStr = R"json({
"attrs":
  {
  "catalog_name":
    "default",
  "db_name":
    "db1",
  "distribution":
    {
    "type":
      "SINGLETON"
    },
  "double_key":
    123.456,
  "source_id":
    "",
  "str_key":
    "string_value",
  "table_distribution":
    {
    "hash_mode":
      {
      "hash_fields":
        [
          "$i1",
          "$i2"
        ],
      "hash_function":
        "HASH"
      },
    "partition_cnt":
      64
    },
  "table_name":
    "t1",
  "table_type":
    "logical"
  },
"id":
  1,
"inputs":
  {
  "input":
    [
      0
    ]
  },
"op_name":
  "ExchangeOp"
})json";

        expectPlanOpStr = formatJsonStr(expectPlanOpStr);

        planOp.patchDoubleAttrs["double_key"] = 123.456;

        PlanOpWrapper planOpWrapper;
        DynamicParams dynamicParamss;
        dynamicParamss._array = dynamicParams;
        DynamicParams innerDynamicParams;
        ASSERT_TRUE(planOpWrapper.convert(planOp, dynamicParamss, innerDynamicParams));
        std::string actualPlanOpStr;
        status = Utils::toJson(planOpWrapper, actualPlanOpStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanOpStr, actualPlanOpStr));
    }

    // patch int64 attr
    {
        std::string expectPlanOpStr = R"json({
"attrs":
  {
  "catalog_name":
    "default",
  "db_name":
    "db1",
  "distribution":
    {
    "type":
      "SINGLETON"
    },
  "double_key":
    123.456,
  "int_key":
    7890,
  "source_id":
    "",
  "str_key":
    "string_value",
  "table_distribution":
    {
    "hash_mode":
      {
      "hash_fields":
        [
          "$i1",
          "$i2"
        ],
      "hash_function":
        "HASH"
      },
    "partition_cnt":
      64
    },
  "table_name":
    "t1",
  "table_type":
    "logical"
  },
"id":
  1,
"inputs":
  {
  "input":
    [
      0
    ]
  },
"op_name":
  "ExchangeOp"
})json";

        expectPlanOpStr = formatJsonStr(expectPlanOpStr);

        planOp.patchInt64Attrs["int_key"] = 7890;

        PlanOpWrapper planOpWrapper;
        DynamicParams dynamicParamss;
        dynamicParamss._array = dynamicParams;
        DynamicParams innerDynamicParams;
        ASSERT_TRUE(planOpWrapper.convert(planOp, dynamicParamss, innerDynamicParams));
        std::string actualPlanOpStr;
        status = Utils::toJson(planOpWrapper, actualPlanOpStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanOpStr, actualPlanOpStr));
    }

    // patch boolean attr
    {
        std::string expectPlanOpStr = R"json({
"attrs":
  {
  "catalog_name":
    "default",
  "db_name":
    "db1",
  "distribution":
    {
    "type":
      "SINGLETON"
    },
  "double_key":
    123.456,
  "false_key":
    false,
  "int_key":
    7890,
  "source_id":
    "",
  "str_key":
    "string_value",
  "table_distribution":
    {
    "hash_mode":
      {
      "hash_fields":
        [
          "$i1",
          "$i2"
        ],
      "hash_function":
        "HASH"
      },
    "partition_cnt":
      64
    },
  "table_name":
    "t1",
  "table_type":
    "logical",
  "true_key":
    true
  },
"id":
  1,
"inputs":
  {
  "input":
    [
      0
    ]
  },
"op_name":
  "ExchangeOp"
})json";

        expectPlanOpStr = formatJsonStr(expectPlanOpStr);

        planOp.patchBoolAttrs["false_key"] = false;
        planOp.patchBoolAttrs["true_key"] = true;

        PlanOpWrapper planOpWrapper;
        DynamicParams dynamicParamss;
        dynamicParamss._array = dynamicParams;
        DynamicParams innerDynamicParams;
        ASSERT_TRUE(planOpWrapper.convert(planOp, dynamicParamss, innerDynamicParams));
        std::string actualPlanOpStr;
        status = Utils::toJson(planOpWrapper, actualPlanOpStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanOpStr, actualPlanOpStr));
    }

    // patch exist object attr
    {
        std::string expectPlanOpStr = R"json({
"attrs":
  {
  "catalog_name":
    "default",
  "db_name":
    "db1",
  "distribution":
    {
    "type":
      "SINGLETON"
    },
  "double_key":
    123.456,
  "false_key":
    false,
  "int_key":
    7890,
  "source_id":
    "",
  "str_key":
    "string_value",
  "table_distribution": "aaa",
  "table_name":
    "t1",
  "table_type":
    "logical",
  "true_key":
    true
  },
"id":
  1,
"inputs":
  {
  "input":
    [
      0
    ]
  },
"op_name":
  "ExchangeOp"
})json";
        expectPlanOpStr = formatJsonStr(expectPlanOpStr);

        planOp.patchStrAttrs["table_distribution"] = std::string("aaa");

        PlanOpWrapper planOpWrapper;
        DynamicParams dynamicParamss;
        dynamicParamss._array = dynamicParams;
        DynamicParams innerDynamicParams;
        ASSERT_TRUE(planOpWrapper.convert(planOp, dynamicParamss, innerDynamicParams));
        std::string actualPlanOpStr;
        status = Utils::toJson(planOpWrapper, actualPlanOpStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanOpStr, actualPlanOpStr));
    }

    // str and int64 patch has same key
    {
        std::string expectPlanOpStr = R"json({
"attrs":
  {
  "catalog_name":
    "default",
  "db_name":
    "db1",
  "distribution":
    {
    "type":
      "SINGLETON"
    },
  "double_key":
    123.456,
  "false_key":
    false,
  "int_key":
    7890,
  "source_id":
    "",
  "str_key":
    "string_value",
  "table_distribution": "aaa",
  "table_name":
    "t1",
  "table_type":
    "logical",
  "true_key":
    true
  },
"id":
  1,
"inputs":
  {
  "input":
    [
      0
    ]
  },
"op_name":
  "ExchangeOp"
})json";
        expectPlanOpStr = formatJsonStr(expectPlanOpStr);

        planOp.patchInt64Attrs["table_distribution"] = 111;

        PlanOpWrapper planOpWrapper;
        DynamicParams dynamicParamss;
        dynamicParamss._array = dynamicParams;
        DynamicParams innerDynamicParams;
        ASSERT_TRUE(planOpWrapper.convert(planOp, dynamicParamss, innerDynamicParams));
        std::string actualPlanOpStr;
        status = Utils::toJson(planOpWrapper, actualPlanOpStr);
        ASSERT_TRUE(status.ok()) << status.errorMessage();

        ASSERT_NO_FATAL_FAILURE(
            autil::legacy::JsonTestUtil::checkJsonStringEqual(expectPlanOpStr, actualPlanOpStr));
    }
}

TEST_F(ResponseTest, testPlanOpHandle) {
    std::vector<autil::legacy::Any> dynamicParam;
    dynamicParam.push_back(autil::legacy::Any(std::string("test_str")));
    dynamicParam.push_back(autil::legacy::Any(100));
    dynamicParam.push_back(autil::legacy::Any(100.123));
    dynamicParam.push_back(autil::legacy::Any(std::string("100")));
    DynamicParams dynamicParams;
    dynamicParams._array.push_back(dynamicParam);
    DynamicParams innerDynamicParams;
    innerDynamicParams.reserveOneSqlParams();
    innerDynamicParams.addKVParams("key_0", autil::legacy::Any(std::string("replace_str")));
    // simple
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "hello";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        std::string expectStr = "\"" + str + "\"";
        ASSERT_EQ(expectStr, sb.GetString());
    }

    // only match prefix
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?0x4141deadbeefiiiii";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        std::string expectStr = "\"" + str + "\"";
        ASSERT_EQ(expectStr, sb.GetString());
    }

    // index is not valid number
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?not_number#int32]]dynamic_params]";
        bool hasException = false;
        try {
            handle.String(str.c_str(), str.size(), true);
        } catch (const IquanException &e) {
            ASSERT_EQ(IQUAN_FAIL, e.code());
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }

    // index is empty
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?#int32]]dynamic_params]";
        bool hasException = false;
        try {
            handle.String(str.c_str(), str.size(), true);
        } catch (const IquanException &e) {
            ASSERT_EQ(IQUAN_FAIL, e.code());
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }

    // type is not valid
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?1#not_type]]dynamic_params]";
        bool hasException = false;
        try {
            handle.String(str.c_str(), str.size(), true);
        } catch (const IquanException &e) {
            ASSERT_EQ(IQUAN_FAIL, e.code());
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }

    // type is empty
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?1#]]dynamic_params]";
        bool hasException = false;
        try {
            handle.String(str.c_str(), str.size(), true);
        } catch (const IquanException &e) {
            ASSERT_EQ(IQUAN_FAIL, e.code());
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }

    // seperator is not valid
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?1$int32]]dynamic_params]";
        bool hasException = false;
        try {
            handle.String(str.c_str(), str.size(), true);
        } catch (const IquanException &e) {
            ASSERT_EQ(IQUAN_FAIL, e.code());
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }

    // index is greater than actual dynamic params size
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?100#int32]]dynamic_params]";
        bool hasException = false;
        try {
            handle.String(str.c_str(), str.size(), true);
        } catch (const IquanException &e) {
            ASSERT_EQ(IQUAN_FAIL, e.code());
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }

    // expected type is not match to actual type of dynamic params, and bad value
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?0#int32]]dynamic_params]";
        bool hasException = false;
        try {
            handle.String(str.c_str(), str.size(), true);
        } catch (const IquanException &e) {
            ASSERT_EQ(IQUAN_FAIL, e.code());
            hasException = true;
        }
        ASSERT_TRUE(hasException);
    }

    // disable replacement
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, nullptr, nullptr);
        std::string str = "[dynamic_params[[?0#string]]dynamic_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ("\"" + str + "\"", sb.GetString());
    }
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, nullptr, nullptr);
        std::string str = "[replace_params[[?0#string]]replace_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ("\"" + str + "\"", sb.GetString());
    }
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, nullptr, nullptr);
        std::string str = "[hint_params[partition]hint_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ("\"" + str + "\"", sb.GetString());
    }

    // normal case
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?0#string]]dynamic_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ(std::string("\"test_str\""), sb.GetString());
    }
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?1#int32]]dynamic_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ(std::string("100"), sb.GetString());
    }
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?1#string]]dynamic_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ(std::string("\"100\""), sb.GetString());
    }
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?2#double]]dynamic_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ(std::string("100.123"), sb.GetString());
    }
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?2#string]]dynamic_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ(std::string("\"100.123\""), sb.GetString());
    }
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?3#int32]]dynamic_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ(std::string("100"), sb.GetString());
    }
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[dynamic_params[[?3#string]]dynamic_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ(std::string("\"100\""), sb.GetString());
    }
    // replace params
    {
        rapidjson::StringBuffer sb;
        autil::legacy::RapidWriter writer(sb);
        PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

        std::string str = "[replace_params[[key_0#string]]replace_params]";
        bool ret = handle.String(str.c_str(), str.size(), true);
        ASSERT_TRUE(ret);
        ASSERT_EQ(std::string("\"replace_str\""), sb.GetString());
    }
}

TEST_F(ResponseTest, testPlanOpHandle_parseDynamicParamsContent_prefixStrNotFound) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    std::string keyStr;
    std::string typeStr;
    autil::StringView planStr("[dynamic_params[[?3#string]]dynamic_params]");
    ASSERT_FALSE(handle.parseDynamicParamsContent(planStr, "xxxx", "xxxx", keyStr, typeStr));
}

TEST_F(ResponseTest, testPlanOpHandle_parseDynamicParamsContent_suffixStrNotFound) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    std::string keyStr;
    std::string typeStr;
    autil::StringView planStr("[dynamic_params[[?3#string]]dynamic_params]");
    ASSERT_FALSE(
        handle.parseDynamicParamsContent(planStr, "[dynamic_params[[?", "xxxx", keyStr, typeStr));
}

TEST_F(ResponseTest, testPlanOpHandle_parseDynamicParamsContent_separatorNotFound) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    std::string keyStr;
    std::string typeStr;
    autil::StringView planStr("[dynamic_params[[?3@string]]dynamic_params]");
    bool hasException = false;
    try {
        handle.parseDynamicParamsContent(
            planStr, "[dynamic_params[[?", "]]dynamic_params]", keyStr, typeStr);
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        hasException = true;
        ASSERT_EQ("can not find # in 3@string", std::string(e.what()));
    }
    ASSERT_TRUE(hasException);
}

TEST_F(ResponseTest, testPlanOpHandle_parseDynamicParamsContent_paramKeyEmpty) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    std::string keyStr;
    std::string typeStr;
    autil::StringView planStr("[dynamic_params[[?#int32]]dynamic_params]");
    bool hasException = false;
    try {
        handle.parseDynamicParamsContent(
            planStr, "[dynamic_params[[?", "]]dynamic_params]", keyStr, typeStr);
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        hasException = true;
        ASSERT_EQ("key is empty, #int32", std::string(e.what()));
    }
    ASSERT_TRUE(hasException);
}

TEST_F(ResponseTest, testPlanOpHandle_parseDynamicParamsContent_paramTypeEmpty) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    std::string keyStr;
    std::string typeStr;
    autil::StringView planStr("[dynamic_params[[?3#]]dynamic_params]");
    bool hasException = false;
    try {
        handle.parseDynamicParamsContent(
            planStr, "[dynamic_params[[?", "]]dynamic_params]", keyStr, typeStr);
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        hasException = true;
        ASSERT_EQ("type is empty, 3#", std::string(e.what()));
    }
    ASSERT_TRUE(hasException);
}

TEST_F(ResponseTest, testPlanOpHandle_parseDynamicParamsContent_succeed) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    std::string keyStr;
    std::string typeStr;
    autil::StringView planStr("[dynamic_params[[?3#string]]dynamic_params]");
    ASSERT_TRUE(handle.parseDynamicParamsContent(
        planStr, "[dynamic_params[[?", "]]dynamic_params]", keyStr, typeStr));
    ASSERT_EQ("3", keyStr);
    ASSERT_EQ("string", typeStr);
}

TEST_F(ResponseTest, testPlanOpHandle_doReplaceParams_parseParamsFailed) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    autil::StringView planStr("[dynamic_params[[?3#string]]dynamic_params]");
    ASSERT_FALSE(handle.doReplaceParams(planStr, true));
}

TEST_F(ResponseTest, testPlanOpHandle_doReplaceParams_replaceKeyNotFound) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    autil::StringView planStr("[replace_params[[3#string]]replace_params]");
    bool hasException = false;
    try {
        ASSERT_FALSE(handle.doReplaceParams(planStr, true));
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        hasException = true;
        ASSERT_EQ("key [3] not found in dynamic params", std::string(e.what()));
    }
    ASSERT_TRUE(hasException);
}

TEST_F(ResponseTest, testPlanOpHandle_doReplaceParams_succeed) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    innerDynamicParams.reserveOneSqlParams();
    innerDynamicParams.addKVParams("3", autil::legacy::Any(std::string("test_str")));
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);

    autil::StringView planStr("[replace_params[[3#string]]replace_params]");
    ASSERT_TRUE(handle.doReplaceParams(planStr, true));
    ASSERT_EQ(std::string("\"test_str\""), sb.GetString());
}

TEST_F(ResponseTest, testPlanOpHandle_doReplaceDynamicParams_parseParamsFailed) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    autil::StringView planStr("[replace_params[[3#string]]replace_params]");
    ASSERT_FALSE(handle.doReplaceDynamicParams(planStr, true));
}

TEST_F(ResponseTest, testPlanOpHandle_doReplaceDynamicParams_keyToIndexFailed) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    autil::StringView planStr("[dynamic_params[[?x#string]]dynamic_params]");
    bool hasException = false;
    try {
        handle.doReplaceDynamicParams(planStr, true);
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        hasException = true;
        ASSERT_EQ("indexStr x can not cast to uint32", std::string(e.what()));
    }
    ASSERT_TRUE(hasException);
}

TEST_F(ResponseTest, testPlanOpHandle_doReplaceDynamicParams_indexOverParamsSize) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    autil::StringView planStr("[dynamic_params[[?3#string]]dynamic_params]");
    bool hasException = false;
    try {
        handle.doReplaceDynamicParams(planStr, true);
    } catch (const IquanException &e) {
        ASSERT_EQ(IQUAN_FAIL, e.code());
        hasException = true;
        ASSERT_EQ("dynamic params idx out of range", std::string(e.what()));
    }
    ASSERT_TRUE(hasException);
}

TEST_F(ResponseTest, testPlanOpHandle_doReplaceDynamicParams_succeed) {
    DynamicParams dynamicParams;
    DynamicParams innerDynamicParams;
    dynamicParams._array = {{autil::legacy::Any(std::string("test_str"))}};
    rapidjson::StringBuffer sb;
    autil::legacy::RapidWriter writer(sb);
    PlanOp::PlanOpHandle handle(writer, &dynamicParams, &innerDynamicParams);
    autil::StringView planStr("[dynamic_params[[?0#string]]dynamic_params]");
    ASSERT_TRUE(handle.doReplaceDynamicParams(planStr, true));
    ASSERT_EQ(std::string("\"test_str\""), sb.GetString()) << sb.GetString();
}

} // namespace iquan
