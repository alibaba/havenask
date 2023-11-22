#include <algorithm>
#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "autil/Log.h"
#include "autil/NoCopyable.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/base/Status.h"
#include "indexlib/config/BuildConfig.h"
#include "indexlib/config/OnlineConfig.h"
#include "indexlib/config/TabletOptions.h"
#include "indexlib/config/TabletSchema.h"
#include "indexlib/config/test/schema_maker.h"
#include "indexlib/framework/ITablet.h"
#include "indexlib/framework/ITabletReader.h"
#include "indexlib/framework/IndexRoot.h"
#include "indexlib/framework/Locator.h"
#include "indexlib/table/kv_table/test/KVTableTestHelper.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/data/TableData.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/resource/testlib/MockTabletManagerR.h"
#include "suez/turing/common/JoinConfigInfo.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace indexlibv2;
using namespace indexlibv2::framework;
using namespace indexlibv2::index;

namespace sql {

class LookupJoinKernelWithKVTest : public OpTestBase {
public:
    LookupJoinKernelWithKVTest();
    ~LookupJoinKernelWithKVTest();

public:
    void setUp() override {
        _tableName = "kv";

        _mockTabletManagerR.reset(new MockTabletManagerR);
        EXPECT_CALL(*_mockTabletManagerR, getTablet(_tableName))
            .WillRepeatedly(testing::Invoke(
                [&](const std::string &tableName) { return _tabletMap[tableName]; }));
        _needBuildTablet = true;
        _needBuildIndex = false;
        _needExprResource = true;
        _needKvTable = true;
        buildNodeWithCompareOp
            = "{\"table_name\":\"kv\",\"db_name\":\"default\",\"catalog_name\":\"default\",\"index_"
              "infos\":{\"$aid\":{\"type\":\"primary_key\",\"name\":\"aid\"}},\"output_fields_"
              "internal\":["
              "\"$aid\",\"$path\",\"$group\",\"$attr\",\"$group2\",\"$attr2\"],\"output_fields_"
              "internal_"
              "type\":[\"BIGINT\",\"VARCHAR\",\"VARCHAR\",\"INTEGER\",\"VARCHAR\",\"INTEGER\"],"
              "\"use_nest_table\":false,\"table_type\":\"kv\",\"batch_size\":1,\"hash_type\":"
              "\"HASH\",\"used_fields\":[\"$aid\",\"$path\",\"$group\",\"$attr\",\"$group2\",\"$"
              "attr2\"],\"hash_fields\":[\"$aid\"], "
              "\"condition\":{\"op\":\"<\",\"type\":\"OTHER\",\"params\":[\"$aid\",4]}}";
        buildNodeWithEqualOp
            = "{\"table_name\":\"kv\",\"db_name\":\"default\",\"catalog_name\":\"default\",\"index_"
              "infos\":{\"$aid\":{\"type\":\"primary_key\",\"name\":\"aid\"}},\"output_fields_"
              "internal\":["
              "\"$aid\",\"$path\",\"$group\",\"$attr\",\"$group2\",\"$attr2\"],\"output_fields_"
              "internal_"
              "type\":[\"BIGINT\",\"VARCHAR\",\"VARCHAR\",\"INTEGER\",\"VARCHAR\",\"INTEGER\"],"
              "\"use_nest_table\":false,\"table_type\":\"kv\",\"batch_size\":1,\"hash_type\":"
              "\"HASH\",\"used_fields\":[\"$aid\",\"$path\",\"$group\",\"$attr\",\"$group2\",\"$"
              "attr2\"],\"hash_fields\":[\"$aid\"], "
              "\"condition\":{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$aid\",4]}}";
        buildNodeWithEqualOp2
            = "{\"table_name\":\"kv\",\"db_name\":\"default\",\"catalog_name\":\"default\",\"index_"
              "infos\":{\"$aid\":{\"type\":\"primary_key\",\"name\":\"aid\"}},\"output_fields_"
              "internal\":["
              "\"$aid\",\"$path\",\"$group\",\"$attr\",\"$group2\",\"$attr2\"],\"output_fields_"
              "internal_"
              "type\":[\"BIGINT\",\"VARCHAR\",\"VARCHAR\",\"INTEGER\",\"VARCHAR\",\"INTEGER\"],"
              "\"use_nest_table\":false,\"table_type\":\"kv\",\"batch_size\":1,\"hash_type\":"
              "\"HASH\",\"used_fields\":[\"$aid\",\"$path\",\"$group\",\"$attr\",\"$group2\",\"$"
              "attr2\"],\"hash_fields\":[\"$aid\"], "
              "\"condition\":{\"op\":\"=\",\"type\":\"OTHER\",\"params\":[\"$aid\",2]}}";
    }
    void tearDown() override {
        _attributeMap.clear();
    }

    void prepareTablet() override {
        ASSERT_NO_FATAL_FAILURE(prepareSqlKvTablet());
    }

private:
    string buildNodeWithCompareOp;
    string buildNodeWithEqualOp;
    string buildNodeWithEqualOp2;
    std::shared_ptr<MockTabletManagerR> _mockTabletManagerR;

private:
    void prepareSqlKvTablet() {
        std::string tableName, fields, attributes, docs, pkeyField;
        bool enableTTL;
        int64_t ttl;
        {
            prepareKvPk64TableDataTablet(
                tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
            std::string testPath = _testPath + tableName;
            framework::IndexRoot indexRoot(testPath, testPath);
            auto tabletSchema = indexlib::test::SchemaMaker::MakeKVTabletSchema(
                fields, pkeyField, attributes, ttl);
            tabletSchema->TEST_SetTableName(tableName);
            auto tabletOptions = CreateMultiColumnOptions();
            auto helper = make_shared<indexlibv2::table::KVTableTestHelper>();
            _dependentHolders.push_back(helper);
            ASSERT_TRUE(helper->Open(indexRoot, tabletSchema, std::move(tabletOptions)).IsOK());
            ASSERT_TRUE(helper->Build(docs).IsOK());
            auto tabletPtr = helper->GetITablet();
            ASSERT_TRUE(_tabletMap.insert(make_pair(tableName, tabletPtr)).second);
        }
    }

    void prepareJoinAttribute(string joinType,
                              bool kvTableIsLeft = false,
                              string remainCondition = "",
                              size_t batchSize = 5,
                              const string &buildNode = "") {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = kvTableIsLeft;
        _attributeMap["join_type"] = joinType;
        if (joinType == "SEMI" || joinType == "ANTI") {
            _attributeMap["semi_join_type"] = joinType;
        }
        _attributeMap["batch_size"] = batchSize;
        _attributeMap["lookup_batch_size"] = batchSize;
        _attributeMap["left_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr", "$group2", "$attr2"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr", "$group2", "$attr2"])json");
        string kvTableMeta = kvTableIsLeft ? "left_table_meta" : "right_table_meta";
        _attributeMap[kvTableMeta] = ParseJson(
            R"json({"field_meta":[{"index_type":"primary_key", "index_name":"aid", "field_name": "$aid", "field_type": "int32"}]})json");
        _attributeMap["system_field_num"] = Any(0);
        if (joinType == "SEMI" || joinType == "ANTI") {
            _attributeMap["output_fields"]
                = ParseJson(R"json(["$aid", "$path", "$group", "$attr", "$group2", "$attr2"])json");
            _attributeMap["output_fields_internal"] = ParseJson(
                R"json(["$aid", "$path", "$group", "$attr", "$group2", "$attr2", "$aid0", "$path0", "$group0", "$attr0", "$group20", "$attr20"])json");
        } else {
            _attributeMap["output_fields"] = ParseJson(
                R"json(["$aid", "$path", "$group", "$attr", "$group2", "$attr2", "$aid0", "$path0", "$group0", "$attr0", "$group20", "$attr20"])json");
        }
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(
            R"json({"op":"AND","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$aid","$aid0"]},{"op":"=","type":"OTHER","params":["$group","$group0"]},{"op":"=","type":"OTHER","params":["$attr","$attr0"]},{"op":"=","type":"OTHER","params":["$group2","$group20"]},{"op":"=","type":"OTHER","params":["$attr2","$attr20"]}]})json");
        if (remainCondition.length() > 0) {
            _attributeMap["remaining_condition"] = ParseJson(remainCondition);
        } else {
            _attributeMap["remaining_condition"]
                = ParseJson(R"json({"op":"<","type":"OTHER","params":["$aid",5]})json");
        }
        if (buildNode.empty()) {
            _attributeMap["build_node"] = ParseJson(
                R"json({"table_name":"kv","db_name":"default","catalog_name":"default","table_meta":{"field_meta":[{"index_type":"primary_key", "index_name":"aid", "field_name": "$aid", "field_type": "int32"}]},"output_fields_internal":["$aid","$path","$group","$attr","$group2","$attr2"],"output_fields_internal_type":["BIGINT","VARCHAR","VARCHAR","INTEGER","VARCHAR","INTEGER"],"use_nest_table":false,"table_type":"kv","batch_size":1,"hash_type":"HASH","used_fields":["$aid","$path","$group","$attr","$group2","$attr2"],"hash_fields":["$aid"]})json");
        } else {
            _attributeMap["build_node"] = ParseJson(buildNode);
        }
    }

    void prepareKvPk64TableDataTablet(std::string &tableName,
                                      std::string &fields,
                                      std::string &pkeyField,
                                      std::string &attributes,
                                      std::string &docs,
                                      bool &enableTTL,
                                      int64_t &ttl) {
        tableName = _tableName;
        fields
            = "path:string;group:string;attr:int32;group2:string:true;attr2:int32:true;aid:int64";
        attributes = "path;group;attr;group2;attr2";
        docs = "cmd=add,path=/a,attr=1,group=groupA,attr2=1 11,group2=groupA group2A,aid=0;"
               "cmd=add,path=/a/b,attr=1,group=groupA,attr2=1 11,group2=groupA group2A,aid=1;"
               "cmd=add,path=/a/b,attr=2,group=groupB,attr2=2 22,group2=groupB group2B,aid=2;"
               "cmd=add,path=/a/b/c,attr=2,group=groupB,attr2=2 22,group2=groupB group2B,aid=3;"
               "cmd=add,path=/a/d/c,attr=3,group=groupA,attr2=3 33,group2=groupA group2A,aid=4;"
               "cmd=add,path=/a/c/c,attr=1,group=groupA,attr2=1 11,group2=groupA group2A,aid=5;"
               "cmd=add,path=/a/c,attr=2,group=groupB,attr2=2 22,group2=groupB group2B,aid=6";
        pkeyField = "aid";
        enableTTL = false;
        ttl = std::numeric_limits<int64_t>::max();
    }

    std::shared_ptr<config::TabletOptions> CreateMultiColumnOptions() {
        std::string jsonStr = R"( {
    "online_index_config": {
        "build_config": {
            "sharding_column_num" : 2,
            "level_num" : 3
        }
    }
    } )";
        auto tabletOptions = std::make_shared<indexlibv2::config::TabletOptions>();
        FastFromJsonString(*tabletOptions, jsonStr);
        tabletOptions->SetIsOnline(true);
        tabletOptions->TEST_GetOnlineConfig().TEST_GetBuildConfig().TEST_SetBuildingMemoryLimit(
            64 * 1024 * 1024);
        return tabletOptions;
    }

    void prepareInputTableForJoin(KernelTesterPtr &testerPtr,
                                  const string &joinType,
                                  bool kvTableIsLeft = false,
                                  const string &remainCondition = "",
                                  bool clearAll = false,
                                  size_t batchSize = 5,
                                  const string &buildNode = "") {
        prepareJoinAttribute(joinType, kvTableIsLeft, remainCondition, batchSize, buildNode);
        testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        vector<MatchDoc> table = _matchDocUtil.createMatchDocs(_allocator, 6);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, table, "aid", {0, 2, 3, 4, 6, 10}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, table, "path", {"-/a", "-/a/b", "-/a/b/c", "-/a/d/c", "-/a/c", "-hehehe"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, table, "attr", {1, 1, 2, 3, 2, 10000}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator,
            table,
            "group",
            {"groupA", "groupB", "groupB", "groupA", "groupB", "hehehe"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, table, "attr2", {11, 11, 22, 33, 22, 10000}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator,
            table,
            "group2",
            {"group2A", "group2B", "group2B", "group2A", "group2B", "hehehe"}));
        auto inputTable = createTable(_allocator, table);
        if (clearAll) {
            TablePtr tablePtr = dynamic_pointer_cast<TableData>(inputTable)->getTable();
            tablePtr->clearFrontRows(6);
            ASSERT_TRUE(0 == tablePtr->getRowCount());
        }
        ASSERT_TRUE(testerPtr->setInput("input0", inputTable, true));
        _inputData = inputTable;
    }
    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        // builder.logLevel("TRACE3");
        builder.resource(_mockTabletManagerR);
        builder.kernel("sql.LookupJoinKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
    }

    void checkOutput(const vector<string> &expect, DataPtr data, const string &field) {
        TablePtr outputTable = getTable(data);
        ASSERT_TRUE(outputTable != NULL);
        auto rowCount = outputTable->getRowCount();
        ASSERT_EQ(expect.size(), rowCount);
        auto column = outputTable->getColumn(field);
        auto name = column->getColumnSchema()->getName();
        ASSERT_TRUE(column != NULL);
        for (size_t i = 0; i < rowCount; i++) {
            // cout << name << " : " << column->toString(i) << endl;
            ASSERT_EQ(expect[i], column->toString(i)) << TableUtil::toString(outputTable);
        }
    }

public:
    MatchDocAllocatorPtr _allocator;

private:
    navi::DataPtr _inputData;

protected:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, LookupJoinKernelWithKVTest);

LookupJoinKernelWithKVTest::LookupJoinKernelWithKVTest() {}

LookupJoinKernelWithKVTest::~LookupJoinKernelWithKVTest() {}

TEST_F(LookupJoinKernelWithKVTest, testInnerJoinKvAsRight) {
    // 1. batch_size = 5, no build_node condition
    {
        KernelTesterPtr testerPtr;
        ASSERT_NO_FATAL_FAILURE(prepareInputTableForJoin(testerPtr, "INNER", false, "", false));

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4" /*, "6" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid0"));
        vector<string> expPath = {"-/a", "-/a/b/c", "-/a/d/c" /*, "-/a/c" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"/a", "/a/b/c", "/a/d/c" /* , "/a/c" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2", "3" /* , "2" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB", "groupA" /* , "groupB" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group0"));
        vector<string> expAttr2 = {"11", "22", "33" /* , "22" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"1^]11", "2^]22", "3^]33" /* , "2^]22" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2 = {"group2A", "group2B", "group2A" /* , "group2B" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20
            = {"groupA^]group2A", "groupB^]group2B", "groupA^]group2A" /* , "groupB^]group2B" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    // 2. batch_size = 1000, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "INNER", false, "", false, 1000, "");

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4" /*, "6" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid0"));
        vector<string> expPath = {"-/a", "-/a/b/c", "-/a/d/c" /*, "-/a/c" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"/a", "/a/b/c", "/a/d/c" /* , "/a/c" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2", "3" /* , "2" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB", "groupA" /* , "groupB" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group0"));
        vector<string> expAttr2 = {"11", "22", "33" /* , "22" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"1^]11", "2^]22", "3^]33" /* , "2^]22" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2 = {"group2A", "group2B", "group2A" /* , "group2B" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20
            = {"groupA^]group2A", "groupB^]group2B", "groupA^]group2A" /* , "groupB^]group2B" */};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    // 3. batch_size = 1000, has build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(
            testerPtr, "INNER", false, "", false, 1000, buildNodeWithCompareOp);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid0"));
        vector<string> expPath = {"-/a", "-/a/b/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"/a", "/a/b/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group0"));
        vector<string> expAttr2 = {"11", "22"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"1^]11", "2^]22"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2 = {"group2A", "group2B"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20 = {"groupA^]group2A", "groupB^]group2B"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "INNER", false, "", true);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);

        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(12, outputTable->getColumnCount());
    }
}

TEST_F(LookupJoinKernelWithKVTest, testInnerJoinKvAsLeft) {
    // 1. batch_size = 5, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "INNER", true, "", false);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid0"));
        vector<string> expPath = {"/a", "/a/b/c", "/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"-/a", "-/a/b/c", "-/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB", "groupA"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group0"));
        vector<string> expAttr2 = {"1^]11", "2^]22", "3^]33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"11", "22", "33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2 = {"groupA^]group2A", "groupB^]group2B", "groupA^]group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20 = {"group2A", "group2B", "group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    // 2. batch_size = 1000, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "INNER", true, "", false, 1000, "");

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid0"));
        vector<string> expPath = {"/a", "/a/b/c", "/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"-/a", "-/a/b/c", "-/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB", "groupA"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group0"));
        vector<string> expAttr2 = {"1^]11", "2^]22", "3^]33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"11", "22", "33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2 = {"groupA^]group2A", "groupB^]group2B", "groupA^]group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20 = {"group2A", "group2B", "group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    // 3. batch_size = 1000, has build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "INNER", true, "", false, 1000, buildNodeWithCompareOp);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid0"));
        vector<string> expPath = {"/a", "/a/b/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"-/a", "-/a/b/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group0"));
        vector<string> expAttr2 = {"1^]11", "2^]22"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"11", "22"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2 = {"groupA^]group2A", "groupB^]group2B"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20 = {"group2A", "group2B"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "INNER", true, "", true);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);

        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(12, outputTable->getColumnCount());
    }
}

TEST_F(LookupJoinKernelWithKVTest, testLeftJoinKvAsRight) {
    // 1. batch_size = 5, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "LEFT", false, "", false);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid0 = {"0", "3", "4", "0", "0", "0"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid0, odata, "aid0"));
        vector<string> expPath = {"-/a", "-/a/b/c", "-/a/d/c", "-/a/b", "-/a/c", "-hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"/a", "/a/b/c", "/a/d/c", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2", "3", "1", "2", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expAttr0 = {"1", "2", "3", "0", "0", "0"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr0, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB", "groupA", "groupB", "groupB", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expGroup0 = {"groupA", "groupB", "groupA", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup0, odata, "group0"));
        vector<string> expAttr2 = {"11", "22", "33", "11", "22", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"1^]11", "2^]22", "3^]33", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2
            = {"group2A", "group2B", "group2A", "group2B", "group2B", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20
            = {"groupA^]group2A", "groupB^]group2B", "groupA^]group2A", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    // 2. batch_size = 1000, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "LEFT", false, "", false, 1000, "");

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4", "2", "6", "10"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expAid0 = {"0", "3", "4", "0", "0", "0"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid0, odata, "aid0"));
        vector<string> expPath = {"-/a", "-/a/b/c", "-/a/d/c", "-/a/b", "-/a/c", "-hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"/a", "/a/b/c", "/a/d/c", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2", "3", "1", "2", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expAttr0 = {"1", "2", "3", "0", "0", "0"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr0, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB", "groupA", "groupB", "groupB", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expGroup0 = {"groupA", "groupB", "groupA", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup0, odata, "group0"));
        vector<string> expAttr2 = {"11", "22", "33", "11", "22", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"1^]11", "2^]22", "3^]33", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2
            = {"group2A", "group2B", "group2A", "group2B", "group2B", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20
            = {"groupA^]group2A", "groupB^]group2B", "groupA^]group2A", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    // 3. batch_size = 1000, has build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "LEFT", false, "", false, 1000, buildNodeWithCompareOp);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "2", "4", "6", "10"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expAid0 = {"0", "3", "0", "0", "0", "0"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid0, odata, "aid0"));
        vector<string> expPath = {"-/a", "-/a/b/c", "-/a/b", "-/a/d/c", "-/a/c", "-hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"/a", "/a/b/c", "", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"1", "2", "1", "3", "2", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expAttr0 = {"1", "2", "0", "0", "0", "0"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr0, odata, "attr0"));
        vector<string> expGroup = {"groupA", "groupB", "groupB", "groupA", "groupB", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expGroup0 = {"groupA", "groupB", "", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup0, odata, "group0"));
        vector<string> expAttr2 = {"11", "22", "11", "33", "22", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"1^]11", "2^]22", "", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2
            = {"group2A", "group2B", "group2B", "group2A", "group2B", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20 = {"groupA^]group2A", "groupB^]group2B", "", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "LEFT", false, "", true);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(12, outputTable->getColumnCount());
    }
}

TEST_F(LookupJoinKernelWithKVTest, testLeftJoinKvAsLeft) {
    // 1. batch_size = 5, no build_node condition
    {
        prepareJoinAttribute("LEFT", true, "", 5, "");
        KernelTesterPtr testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }
    /**
    // 2. batch_size = 1000, has build_node condition
    {
        prepareJoinAttribute("LEFT", true, "", 1000, buildNodeWithCompareOp);
        KernelTesterPtr testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }
    // 3. batch_size = 1000, has build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "LEFT", true, "", false, 1000, buildNodeWithEqualOp);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"4"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid0"));
        vector<string> expPath = {"/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"-/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expAttr = {"3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr0"));
        vector<string> expGroup = {"groupA"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group0"));
        vector<string> expAttr2 = {"3^]33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expAttr20 = {"33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr20, odata, "attr20"));
        vector<string> expGroup2 = {"groupA^]group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
        vector<string> expGroup20 = {"group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup20, odata, "group20"));
    }
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "LEFT", true, "", true, 1000, buildNodeWithEqualOp);
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        auto outTable = dynamic_pointer_cast<TableData>(odata)->getTable();
        ASSERT_TRUE(0 == outTable->getRowCount());
        ASSERT_TRUE(12 == outTable->getColumnCount());
    }
    */
}

TEST_F(LookupJoinKernelWithKVTest, testSemiJoinKvAsLeft) {
    // 1. batch_size = 5, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "SEMI", true, "", false);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"/a", "/a/b/c", "/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "2", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupA", "groupB", "groupA"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"1^]11", "2^]22", "3^]33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"groupA^]group2A", "groupB^]group2B", "groupA^]group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    // 2. batch_size = 1000, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "SEMI", true, "", false, 1000, "");

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"/a", "/a/b/c", "/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "2", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupA", "groupB", "groupA"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"1^]11", "2^]22", "3^]33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"groupA^]group2A", "groupB^]group2B", "groupA^]group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    // 3. batch_size = 1000, has build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "SEMI", true, "", false, 1000, buildNodeWithCompareOp);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"/a", "/a/b/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "2"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupA", "groupB"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"1^]11", "2^]22"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"groupA^]group2A", "groupB^]group2B"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "SEMI", true, "", true);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(6, outputTable->getColumnCount());
    }
}

TEST_F(LookupJoinKernelWithKVTest, testSemiJoinKvAsRight) {
    // 1. batch_size = 5, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "SEMI", false, "", false);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"-/a", "-/a/b/c", "-/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "2", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupA", "groupB", "groupA"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"11", "22", "33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"group2A", "group2B", "group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    // 2. batch_size = 1000, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "SEMI", false, "", false, 1000, "");

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "4"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"-/a", "-/a/b/c", "-/a/d/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "2", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupA", "groupB", "groupA"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"11", "22", "33"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"group2A", "group2B", "group2A"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    // 3. batch_size = 1000, has build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "SEMI", false, "", false, 1000, buildNodeWithCompareOp);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"-/a", "-/a/b/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "2"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupA", "groupB"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"11", "22"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"group2A", "group2B"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "SEMI", false, "", true);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);

        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(6, outputTable->getColumnCount());
    }
}

TEST_F(LookupJoinKernelWithKVTest, testAntiJoinKvAsRight) {
    // 1. batch_size = 5, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "ANTI", false, "", false);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"2", "6", "10"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"-/a/b", "-/a/c", "-hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "2", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupB", "groupB", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"11", "22", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"group2B", "group2B", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    // 2. batch_size = 1000, no build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "ANTI", false, "", false, 1000, "");

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"2", "6", "10"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"-/a/b", "-/a/c", "-hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "2", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupB", "groupB", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"11", "22", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"group2B", "group2B", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    // 3. batch_size = 1000, has build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "ANTI", false, "", false, 1000, buildNodeWithCompareOp);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"2", "4", "6", "10"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"-/a/b", "-/a/d/c", "-/a/c", "-hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"1", "3", "2", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupB", "groupA", "groupB", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"11", "33", "22", "10000"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"group2B", "group2A", "group2B", "hehehe"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "ANTI", false, "", true);

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);

        ASSERT_EQ(0, outputTable->getRowCount());
        ASSERT_EQ(6, outputTable->getColumnCount());
    }
}

TEST_F(LookupJoinKernelWithKVTest, testAntiJoinKvAsLeft) {
    // 1. batch_size = 5, no build_node condition
    {
        prepareJoinAttribute("ANTI", true, "", 5, "");
        KernelTesterPtr testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }
    /**
    // 2. batch_size = 1000, has build_node condition
    {
        prepareJoinAttribute("ANTI", true, "", 1000, buildNodeWithCompareOp);
        KernelTesterPtr testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }
    // 3. batch_size = 1000, has build_node condition
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "ANTI", true, "", false, 1000, buildNodeWithEqualOp2);
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        vector<string> expAid = {"2"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expPath = {"/a/b"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expAttr = {"2"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr, odata, "attr"));
        vector<string> expGroup = {"groupB"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
        vector<string> expAttr2 = {"2^]22"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAttr2, odata, "attr2"));
        vector<string> expGroup2 = {"groupB^]group2B"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup2, odata, "group2"));
    }
    {
        KernelTesterPtr testerPtr;
        prepareInputTableForJoin(testerPtr, "ANTI", true, "", true, 1000, buildNodeWithEqualOp2);
        ASSERT_TRUE(testerPtr->compute()); // kernel compute success
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
        DataPtr odata;
        bool eof = false;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        auto outTable = dynamic_pointer_cast<TableData>(odata)->getTable();
        ASSERT_TRUE(0 == outTable->getRowCount());
        ASSERT_TRUE(6 == outTable->getColumnCount());
    }
    */
}

} // namespace sql
