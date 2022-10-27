#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/scan/ScanKernel.h>
#include <ha3/turing/ops/SqlSessionResource.h>
#include <build_service/analyzer/AnalyzerFactory.h>
#include <build_service/analyzer/AnalyzerInfos.h>
#include <build_service/analyzer/TokenizerManager.h>
#include <navi/resource/MemoryPoolResource.h>
using namespace std;
using namespace suez::turing;
using namespace build_service::analyzer;
using namespace navi;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class ScanBaseTest : public OpTestBase {
public:
    ScanBaseTest();
    ~ScanBaseTest();
public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
    }
    void tearDown() override {
    }
};

ScanBaseTest::ScanBaseTest() {
}

ScanBaseTest::~ScanBaseTest() {
}

TEST_F(ScanBaseTest, testScanInitParamInit) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = string("test_table");
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["hash_type"] = string("HASH");
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["used_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["use_nest_table"] = Any(false);
    attributeMap["output_field_exprs"] = ParseJson(R"json({"$attr1":{"op":"+", "params":["$attr1", 1]}})json");
    attributeMap["condition"] = ParseJson(R"json({"op":"LIKE", "params":["$index2", "a"]})json");
    attributeMap["index_infos"] = ParseJson(string(R"json({"$index2":{"type":"TEXT", "name":"index_2"}})json"));
    attributeMap["hints"] = ParseJson(R"json({"SCAN_ATTR":{"a":"b"}})json");
    attributeMap["limit"] = Any(1000);
    attributeMap["parallel_num"] = Any(10);
    attributeMap["parallel_index"] = Any(1);
    attributeMap["batch_size"] = Any(111);
    attributeMap["unkown"] = Any(1000);
    string jsonStr = autil::legacy::ToJsonString(attributeMap);
    JsonMap jsonMap;
    FromJsonString(jsonMap, jsonStr);
    autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
    ScanInitParam param;
    ASSERT_TRUE(param.initFromJson(wrapper));
    ASSERT_EQ("test_table", param.tableName);
    ASSERT_EQ("normal", param.tableType);
    ASSERT_EQ("HASH", param.hashType);
    ASSERT_THAT(param.outputFields, testing::ElementsAre("$attr1", "$attr2", "$id"));
    ASSERT_THAT(param.hashFields, testing::ElementsAre("$attr1"));
    ASSERT_THAT(param.usedFields, testing::ElementsAre("$attr1", "$attr2", "$id"));
    ASSERT_FALSE(param.useNest);
    EXPECT_EQ("{\"op\":\"LIKE\",\"params\":[\"$index2\",\"a\"]}", param.conditionJson);
    EXPECT_EQ("{\"$attr1\":{\"op\":\"+\",\"params\":[\"$attr1\",1]}}", param.outputExprsJson);
    ASSERT_EQ(1, param.indexInfos.size());
    ASSERT_EQ("index_2", param.indexInfos["$index2"].name);
    ASSERT_EQ("TEXT", param.indexInfos["$index2"].type);
    ASSERT_EQ(1000, param.limit);
    map<string, map<string, string> > expects;
    expects["SCAN_ATTR"] = {{"a", "b"}};
    ASSERT_EQ(expects, param.hintsMap);
    ASSERT_EQ(10, param.parallelNum);
    ASSERT_EQ(1, param.parallelIndex);
    ASSERT_EQ(111, param.batchSize);
}

TEST_F(ScanBaseTest, testScanInitParamInit_EmptyOutputFieldExprs) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_name"] = string("test_table");
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["table_type"] = string("normal");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["$attr1"])json"));
    attributeMap["condition"] = ParseJson(R"json({})json");
    string jsonStr = autil::legacy::ToJsonString(attributeMap);
    JsonMap jsonMap;
    FromJsonString(jsonMap, jsonStr);
    autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
    ScanInitParam param;
    navi::NaviLoggerProvider provider("ERROR");
    ASSERT_TRUE(param.initFromJson(wrapper));
    EXPECT_EQ("{}", param.outputExprsJson);
}

TEST_F(ScanBaseTest, testScanInitParamInitTableDist) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = string("test_table");
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["table_distribution"] = ParseJson(string(R"json({"hash_mode":{"hash_function":"HASH","hash_fields":["$attr1"]}})json"));

    string jsonStr = autil::legacy::ToJsonString(attributeMap);
    JsonMap jsonMap;
    FromJsonString(jsonMap, jsonStr);
    autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
    ScanInitParam param;
    ASSERT_TRUE(param.initFromJson(wrapper));
    ASSERT_EQ("test_table", param.tableName);
    ASSERT_EQ("normal", param.tableType);
    ASSERT_EQ("HASH", param.hashType);
    ASSERT_THAT(param.hashFields, testing::ElementsAre("$attr1"));
}



TEST_F(ScanBaseTest, testScanBaseInit) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["limit"] = Any(1000);
    string jsonStr = autil::legacy::ToJsonString(attributeMap);
    JsonMap jsonMap;
    FromJsonString(jsonMap, jsonStr);
    autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
    ScanInitParam param;
    int64_t timeout = TimeUtility::currentTime()/ 1000 + 1000;
    _sqlQueryResource->setTimeoutMs(timeout);
    ASSERT_TRUE(param.initFromJson(wrapper));
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    param.memoryPoolResource = &_memPoolResource;

    ScanBase scanBase;
    ASSERT_TRUE(scanBase.init(param));
    ASSERT_TRUE(scanBase._timeoutTerminator != nullptr);
    ASSERT_TRUE(scanBase._attributeExpressionCreator != nullptr);
}

TEST_F(ScanBaseTest, testInitWithHint) {
    autil::legacy::json::JsonMap attributeMap;
    attributeMap["table_type"] = string("normal");
    attributeMap["table_name"] = _tableName;
    attributeMap["db_name"] = string("default");
    attributeMap["hints"] = ParseJson(R"json({"SCAN_ATTR":{"localLimit":"10","batchSize":"20"}})json");
    attributeMap["catalog_name"] = string("default");
    attributeMap["hash_fields"] = ParseJson(string(R"json(["id"])json"));
    attributeMap["output_fields"] = ParseJson(string(R"json(["$attr1", "$attr2", "$id"])json"));
    attributeMap["limit"] = Any(1000);
    string jsonStr = autil::legacy::ToJsonString(attributeMap);
    JsonMap jsonMap;
    FromJsonString(jsonMap, jsonStr);
    autil::legacy::Jsonizable::JsonWrapper wrapper(jsonMap);
    ScanInitParam param;
    int64_t timeout = TimeUtility::currentTime()/ 1000 + 1000;
    _sqlQueryResource->setTimeoutMs(timeout);
    ASSERT_TRUE(param.initFromJson(wrapper));
    param.bizResource = _sqlBizResource.get();
    param.queryResource = _sqlQueryResource.get();
    param.memoryPoolResource = &_memPoolResource;

    ScanBase scanBase;
    ASSERT_TRUE(scanBase.init(param));
    ASSERT_TRUE(scanBase._timeoutTerminator != nullptr);
    ASSERT_TRUE(scanBase._attributeExpressionCreator != nullptr);
    ASSERT_EQ(10, scanBase._limit);
    ASSERT_EQ(20, scanBase._batchSize);
}


TEST_F(ScanBaseTest, testPatchHintInfo) {
    // empty map, default value
    {
        ScanBase scanBase;
        map<string, map<string, string> > hintsMap;
        scanBase.patchHintInfo(hintsMap);
        ASSERT_EQ(-1, scanBase._limit);
        ASSERT_EQ(0, scanBase._batchSize);
        ASSERT_EQ(LEFT_JOIN, scanBase._nestTableJoinType);
    }

    // simple test, inner join
    {
        ScanBase scanBase;
        map<string, map<string, string> > hintsMap;
        hintsMap[SQL_SCAN_HINT]["localLimit"] = "100";
        hintsMap[SQL_SCAN_HINT]["batchSize"] = "200";
        hintsMap[SQL_SCAN_HINT]["nestTableJoinType"] = "inner";
        scanBase.patchHintInfo(hintsMap);
        ASSERT_EQ(100, scanBase._limit);
        ASSERT_EQ(200, scanBase._batchSize);
        ASSERT_EQ(INNER_JOIN, scanBase._nestTableJoinType);
    }

    // simple test, left join
    {
        ScanBase scanBase;
        map<string, map<string, string> > hintsMap;
        hintsMap[SQL_SCAN_HINT]["localLimit"] = "100";
        hintsMap[SQL_SCAN_HINT]["batchSize"] = "200";
        hintsMap[SQL_SCAN_HINT]["nestTableJoinType"] = "left";
        scanBase.patchHintInfo(hintsMap);
        ASSERT_EQ(100, scanBase._limit);
        ASSERT_EQ(200, scanBase._batchSize);
        ASSERT_EQ(LEFT_JOIN, scanBase._nestTableJoinType);
    }


    // invalid values
    {
        ScanBase scanBase;
        map<string, map<string, string> > hintsMap;
        hintsMap[SQL_SCAN_HINT]["localLimit"] = "not_a_number";
        hintsMap[SQL_SCAN_HINT]["batchSize"] = "not_a_number";
        hintsMap[SQL_SCAN_HINT]["nestTableJoinType"] = "unknown_type";
        scanBase.patchHintInfo(hintsMap);
        ASSERT_EQ(-1, scanBase._limit);
        ASSERT_EQ(0, scanBase._batchSize);
        ASSERT_EQ(LEFT_JOIN, scanBase._nestTableJoinType);
    }

}


END_HA3_NAMESPACE(sql);
