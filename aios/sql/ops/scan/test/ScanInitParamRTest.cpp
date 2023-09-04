#include "sql/ops/scan/ScanInitParamR.h"

#include <iosfwd>
#include <limits>
#include <map>
#include <stdint.h>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/fast_jsonizable_dec.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/analyzer/Token.h"
#include "ha3/turing/common/ModelConfig.h"
#include "navi/common.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/common/IndexInfo.h"
#include "sql/common/TableMeta.h"
#include "sql/ops/calc/CalcInitParamR.h"
#include "sql/ops/sort/SortInitParam.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/ops/util/KernelUtil.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "table/Row.h"
#include "unittest/unittest.h"

using namespace std;
using namespace suez::turing;
using namespace build_service::analyzer;
using namespace navi;
using namespace autil;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

using namespace isearch::turing;

namespace sql {

class ScanInitParamRTest : public OpTestBase {
public:
    ScanInitParamRTest();
    ~ScanInitParamRTest();
};

ScanInitParamRTest::ScanInitParamRTest() {}

ScanInitParamRTest::~ScanInitParamRTest() {}

TEST_F(ScanInitParamRTest, testScanInitParamInit) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = string("test_table");
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["hash_type"] = string("HASH");
    attributeMap["output_fields_internal"]
        = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["output_fields_internal_type"]
        = ParseJson(string(R"json(["str", "str", "str"])json"));
    attributeMap["used_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["use_nest_table"] = Any(false);
    attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"LIKE", "params":["$index2", "a"]},
            "output_field_exprs": {"$attr1":{"op":"+", "params":["$attr1", 1]}}
        },
        "op_name": "CalcOp"
    }])json");
    attributeMap["table_meta"] = ParseJson(R"json({
        "field_meta": [
            {
                "field_type": "TEXT",
                "field_name": "$index2",
                "index_name": "index_2",
                "index_type": "TEXT"
            }
        ]
    })json");
    attributeMap["hints"] = ParseJson(R"json({"SCAN_ATTR":{"a":"b"}})json");
    attributeMap["limit"] = Any(1000);
    attributeMap["parallel_num"] = Any(10);
    attributeMap["parallel_index"] = Any(1);
    attributeMap["batch_size"] = Any(111);
    attributeMap["unkown"] = Any(1000);

    // agg index
    attributeMap["aggregation_index_name"] = string("agg_index");
    attributeMap["aggregation_keys"] = ParseJson(string(R"json(["123", "456"])json"));
    attributeMap["aggregation_type"] = string("lookup");
    attributeMap["aggregation_value_field"] = string("field");
    attributeMap["aggregation_distinct"] = Any(true);
    attributeMap["aggregation_range_map"] = ParseJson(R"json({
                    "reach_time": ["123","456"]
                })json");

    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(param);
    ASSERT_EQ("test_table", param->tableName);
    ASSERT_EQ("normal", param->tableType);
    ASSERT_EQ("HASH", param->hashType);
    ASSERT_THAT(param->calcInitParamR->outputFields, testing::ElementsAre("attr1", "attr2", "id"));
    ASSERT_THAT(param->calcInitParamR->outputFieldsType, testing::ElementsAre("str", "str", "str"));
    ASSERT_THAT(param->hashFields, testing::ElementsAre("attr1"));
    ASSERT_THAT(param->usedFields, testing::ElementsAre("attr1", "attr2", "id"));
    ASSERT_FALSE(param->useNest);
    EXPECT_EQ("{\"op\":\"LIKE\",\"params\":[\"$index2\",\"a\"]}",
              param->calcInitParamR->conditionJson);
    EXPECT_EQ("{\"$attr1\":{\"op\":\"+\",\"params\":[\"$attr1\",1]}}",
              param->calcInitParamR->outputExprsJson);
    ASSERT_EQ(1, param->indexInfos.size());
    ASSERT_EQ("index_2", param->indexInfos["index2"].name);
    ASSERT_EQ("TEXT", param->indexInfos["index2"].type);
    ASSERT_EQ(1000, param->limit);
    map<string, map<string, string>> expects;
    expects["SCAN_ATTR"] = {{"a", "b"}};
    ASSERT_EQ(expects, param->hintsMap);
    ASSERT_EQ(10, param->parallelNum);
    ASSERT_EQ(1, param->parallelIndex);
    ASSERT_EQ(111, param->batchSize);
    ASSERT_EQ("agg_index", param->aggIndexName);
    ASSERT_EQ(param->aggKeys, std::vector<std::string>({"123", "456"}));
    ASSERT_EQ("lookup", param->aggType);
    ASSERT_EQ("field", param->aggValueField);
    ASSERT_EQ(true, param->aggDistinct);
    std::map<std::string, std::pair<std::string, std::string>> rangeMap {
        {"reach_time", {"123", "456"}}};
    ASSERT_EQ(rangeMap, param->aggRangeMap);
}

TEST_F(ScanInitParamRTest, testScanInit_sortOp1) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["table_name"] = string("test_table");
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "order_fields": ["attr1", "id"],
            "directions": ["ASC", "ASC"],
            "limit": 2,
            "offset": 0
        },
        "op_name": "SortOp"
    }])json");
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(param);
    ASSERT_EQ(0, param->sortDesc.keys.size());
}

TEST_F(ScanInitParamRTest, testScanInit_sortOp2) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["table_name"] = string("test_table");
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"QUERY", "params":["index_2", "'a' OR 'b'"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    },
    {
        "attrs": {
            "order_fields": ["attr1", "id"],
            "directions": ["ASC", "ASC"],
            "limit": 2,
            "offset": 0
        },
        "op_name": "SortOp"
    }])json");
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(param);
    ASSERT_EQ(2, param->sortDesc.limit);
    ASSERT_EQ(0, param->sortDesc.offset);
    ASSERT_EQ(2, param->sortDesc.topk);
    ASSERT_EQ(2, param->sortDesc.keys.size());
    ASSERT_EQ(2, param->sortDesc.orders.size());
}

TEST_F(ScanInitParamRTest, testScanInit_sortOpFailed) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["table_name"] = string("test_table");
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["push_down_ops"] = ParseJson(R"json([
    {
        "attrs": {
            "condition": {"op":"QUERY", "params":["index_2", "'a' OR 'b'"], "type":"UDF"},
            "output_field_exprs": {}
        },
        "op_name": "CalcOp"
    },
    {
        "attrs": {
            "order_fields": [],
            "directions": ["ASC", "ASC"],
            "limit": 2,
            "offset": 0
        },
        "op_name": "SortOp"
    }])json");
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    // navi::NaviLoggerProvider provider("ERROR");
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_FALSE(param);
}

TEST_F(ScanInitParamRTest, testScanInitParamInit_EmptyOutputFieldExprs) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_name"] = string("test_table");
    attributeMap["table_type"] = string("normal");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["condition"] = ParseJson(R"json({})json");
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(param);
    EXPECT_EQ("{}", param->calcInitParamR->outputExprsJson);
}

TEST_F(ScanInitParamRTest, testScanInitParamInitTableDist) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = string("test_table");
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["table_distribution"] = ParseJson(
        string(R"json({"hash_mode":{"hash_function":"HASH","hash_fields":["$attr1"]}})json"));

    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(param);
    ASSERT_EQ("test_table", param->tableName);
    ASSERT_EQ("normal", param->tableType);
    ASSERT_EQ("HASH", param->hashType);
    ASSERT_THAT(param->hashFields, testing::ElementsAre("attr1"));
}

TEST_F(ScanInitParamRTest, testInitWithHint) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["hints"]
        = ParseJson(R"json({"SCAN_ATTR":{"localLimit":"10","batchSize":"20"}})json");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["limit"] = Any(1000);
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(param);
    ASSERT_EQ(10, param->limit);
    ASSERT_EQ(20, param->batchSize);
}

TEST_F(ScanInitParamRTest, testInitWithHint_limit) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["hints"]
        = ParseJson(R"json({"SCAN_ATTR":{"localLimit":"10000","batchSize":"20"}})json");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["limit"] = Any(1000);
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(param);
    ASSERT_EQ(1000, param->limit);
}

TEST_F(ScanInitParamRTest, testInitWithHint_invalidValue) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["hints"] = ParseJson(
        R"json({"SCAN_ATTR":{"localLimit":"not_number","batchSize":"not_number"}})json");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    string jsonStr = autil::legacy::FastToJsonString(attributeMap);
    auto *naviRHelper = getNaviRHelper();
    naviRHelper->kernelConfig(jsonStr);
    auto param = naviRHelper->getOrCreateRes<ScanInitParamR>();
    ASSERT_TRUE(param);
    ASSERT_EQ(std::numeric_limits<uint32_t>::max(), param->limit);
    ASSERT_EQ(0, param->batchSize);
}

TEST_F(ScanInitParamRTest, testForbidIndex) {
    std::map<std::string, std::map<std::string, std::string>> hintsMap;
    autil::legacy::FastFromJsonString(hintsMap, R"json({"SCAN_ATTR":{"forbidIndex":"a,b,c"}})json");
    std::string tableMetaStr = R"json({
        "field_meta": [
            {
                "field_type": "NUMBER",
                "field_name": "id",
                "index_name": "id",
                "index_type": "primarykey64"
            },
            {
                "field_type": "NUMBER",
                "field_name": "a",
                "index_name": "a",
                "index_type": "int64"
            },
            {
                "field_type": "NUMBER",
                "field_name": "b",
                "index_name": "b",
                "index_type": "int64"
            },
            {
                "field_type": "NUMBER",
                "field_name": "c",
                "index_name": "c",
                "index_type": "int64"
            },
            {
                "field_type": "NUMBER",
                "field_name": "d",
                "index_name": "d",
                "index_type": "int64"
            }
        ]
    })json";
    TableMeta tableMeta;
    autil::legacy::FastFromJsonString(tableMeta, tableMetaStr);
    std::map<std::string, IndexInfo> indexInfos;
    tableMeta.extractIndexInfos(indexInfos);
    KernelUtil::stripName(indexInfos);
    ASSERT_EQ(5, indexInfos.size());
    std::unordered_set<std::string> forbidIndexs;
    ScanInitParamR::forbidIndex(hintsMap, indexInfos, forbidIndexs);
    ASSERT_THAT(forbidIndexs, testing::UnorderedElementsAre("a", "b", "c"));
    ASSERT_EQ(2, indexInfos.size());
}

TEST_F(ScanInitParamRTest, testRemoteScanParamInit) {
    std::map<std::string, std::map<std::string, std::string>> hintsMap;
    autil::legacy::FastFromJsonString(hintsMap,
                                      R"json({"SCAN_ATTR":{"remoteSourceType":"xxx"}})json");
    ASSERT_TRUE(ScanInitParamR::isRemoteScan(hintsMap));
}

} // namespace sql
