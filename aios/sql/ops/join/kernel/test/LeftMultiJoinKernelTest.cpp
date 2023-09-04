#include <algorithm>
#include <cstddef>
#include <limits>
#include <map>
#include <memory>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/MultiValueType.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "indexlib/config/index_partition_schema.h"
#include "indexlib/partition/index_application.h"
#include "indexlib/partition/index_partition.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/common/common.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/expression/common.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class LeftMultiJoinKernelTest : public OpTestBase {
public:
    LeftMultiJoinKernelTest() {}

public:
    void setUp() override {
        _tableName = "kv";
        _needBuildIndex = true;
        _needExprResource = true;
        _needKvTable = true;
    }

    void tearDown() override {
        _attributeMap.clear();
    }

    void prepareUserIndex() override {
        prepareSqlKvTable();
    }

protected:
    void prepareSqlKvTable() {
        std::string tableName, fields, attributes, docs, pkeyField;
        bool enableTTL;
        int64_t ttl;
        prepareKvTableData(tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
        std::string testPath = _testPath + tableName;
        auto indexPartition = makeIndexPartitionForKvTable(
            testPath, tableName, fields, pkeyField, attributes, docs, enableTTL, ttl);
        std::string schemaName = indexPartition->GetSchema()->GetSchemaName();
        _indexPartitionMap.insert(make_pair(schemaName, indexPartition));
    }

    void prepareKvTableData(std::string &tableName,
                            std::string &fields,
                            std::string &pkeyField,
                            std::string &attributes,
                            std::string &docs,
                            bool &enableTTL,
                            int64_t &ttl) override {
        tableName = _tableName;
        fields = "path:string;group:string;attr:int32;group2:string;attr2:int32;aid:string";
        attributes = "path;group;attr;group2;attr2";
        docs = "cmd=add,path=/a,attr=1,group=groupA,attr2=11,group2=group2A,aid=0;"
               "cmd=add,path=/a/b,attr=1,group=groupA,attr2=11,group2=group2A,aid=1;"
               "cmd=add,path=/a/b,attr=2,group=groupB,attr2=22,group2=group2B,aid=2;"
               "cmd=add,path=/a/b/c,attr=2,group=groupB,attr2=22,group2=group2B,aid=3;"
               "cmd=add,path=/a/d/c,attr=3,group=groupA,attr2=33,group2=group2A,aid=4;"
               "cmd=add,path=/a/c/c,attr=3,group=groupA,attr2=33,group2=group2A,aid=5;"
               "cmd=add,path=/a/c,attr=4,group=groupB,attr2=44,group2=group2B,aid=6";
        pkeyField = "aid";
        enableTTL = false;
        ttl = std::numeric_limits<int64_t>::max();
    }

    void prepareJoinAttribute(size_t batchSize = 5, const std::string &equiCondition = "") {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = false;
        _attributeMap["join_type"] = SQL_LEFT_MULTI_JOIN_TYPE;
        _attributeMap["batch_size"] = batchSize;
        _attributeMap["left_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr", "$group2", "$attr2"])json");
        _attributeMap["right_input_fields"]
            = ParseJson(R"json(["$aid", "$path", "$group", "$attr", "$group2", "$attr2"])json");
        string kvTableMeta = "right_table_meta";
        _attributeMap[kvTableMeta] = ParseJson(
            R"json({"field_meta":[{"index_type":"primary_key", "index_name":"aid", "field_name": "$aid", "field_type": "string"}]})json");
        _attributeMap["output_fields"] = ParseJson(
            R"json(["$aid", "$path", "$group", "$attr", "$group2", "$attr2", "$aid0", "$path0", "$group0", "$attr0", "$group20", "$attr20"])json");
        if (equiCondition.empty()) {
            _attributeMap["equi_condition"] = _attributeMap["condition"]
                = ParseJson(R"json({"op":">>","type":"OTHER","params":["$aid","$aid0"]})json");
        } else {
            _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(equiCondition);
        }
        _attributeMap["build_node"] = ParseJson(
            R"json({"table_name":"kv","db_name":"default","catalog_name":"default","table_meta":{"field_meta":[{"index_type":"primary_key", "index_name":"aid", "field_name": "$aid", "field_type": "string"}]},"output_fields_internal":["$aid","$path","$group","$attr","$group2","$attr2"],"output_fields_internal_type":["BIGINT","VARCHAR","VARCHAR","INTEGER","VARCHAR","INTEGER"],"use_nest_table":false,"table_type":"kv","batch_size":1,"hash_type":"HASH","used_fields":["$aid","$path","$group","$attr","$group2","$attr2"],"hash_fields":["$aid"]})json");
    }

    void prepareInputTableForJoin(KernelTesterPtr &testerPtr, size_t batchSize = 10) {
        prepareJoinAttribute(batchSize);
        testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        vector<MatchDoc> table = _matchDocUtil.createMatchDocs(_allocator, 6);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
            _allocator,
            table,
            "aid",
            {{"0"}, {"0", "not_found", "2"}, {"3"}, {"4", "5"}, {"5", "4"}, {"10"}}));
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
        ASSERT_TRUE(testerPtr->setInput("input0", inputTable, true));
        _inputData = inputTable;
    }

    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.LeftMultiJoinKernel");
        builder.input("input0");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
    }

private:
    MatchDocAllocatorPtr _allocator;
    navi::DataPtr _inputData;
};

TEST_F(LeftMultiJoinKernelTest, testLeftColumnInvalid) {
    KernelTesterPtr testerPtr;
    prepareJoinAttribute(
        5,
        R"json({"op":"AND","type":"OTHER","params":[{"op":"=","type":"OTHER","params":["$aid","$aid0"]},{"op":"=","type":"OTHER","params":["$group","$group0"]},{"op":"=","type":"OTHER","params":["$attr","$attr0"]},{"op":"=","type":"OTHER","params":["$group2","$group20"]},{"op":"=","type":"OTHER","params":["$attr2","$attr20"]}]})json");
    testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(LeftMultiJoinKernelTest, testLeftIndexInfoInvalid) {
    KernelTesterPtr testerPtr;
    prepareJoinAttribute();
    string kvTableMeta = "right_table_meta";
    _attributeMap[kvTableMeta] = ParseJson(
        R"json({"field_meta":[{"index_type":"primary_key", "index_name":"group", "field_name": "$group", "field_type": "string"}]})json");

    testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(LeftMultiJoinKernelTest, testLeftMultiJoin) {
    KernelTesterPtr testerPtr;
    ASSERT_NO_FATAL_FAILURE(prepareInputTableForJoin(testerPtr, 2));
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(odata != nullptr);
    auto outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != nullptr);
    ASSERT_EQ(6, outputTable->getRowCount());
    ASSERT_EQ(12, outputTable->getColumnCount());
    compareMultiColumn<autil::MultiChar, string>(
        outputTable.get(),
        "aid",
        {{"0"}, {"0", "not_found", "2"}, {"3"}, {"4", "5"}, {"5", "4"}, {"10"}});
    compareSingleColumn<int32_t>(outputTable.get(), "attr", {1, 1, 2, 3, 2, 10000});
    compareStringColumn(
        outputTable.get(), "path", {"-/a", "-/a/b", "-/a/b/c", "-/a/d/c", "-/a/c", "-hehehe"});
    compareMultiColumn<int32_t>(
        outputTable.get(), "attr0", {{1}, {1, 0, 2}, {2}, {3, 3}, {3, 3}, {0}});
    compareMultiColumn<autil::MultiChar, string>(
        outputTable.get(), "aid0", {{"0"}, {"0", "", "2"}, {"3"}, {"4", "5"}, {"5", "4"}, {""}});
    compareMultiColumn<autil::MultiChar, string>(
        outputTable.get(),
        "path0",
        {{"/a"}, {"/a", "", "/a/b"}, {"/a/b/c"}, {"/a/d/c", "/a/c/c"}, {"/a/c/c", "/a/d/c"}, {""}});
}

TEST_F(LeftMultiJoinKernelTest, testLeftMultiJoinBatchSize) {
    KernelTesterPtr testerPtr;
    ASSERT_NO_FATAL_FAILURE(prepareInputTableForJoin(testerPtr, 1000));
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    DataPtr odata;
    bool eof = false;
    ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
    ASSERT_TRUE(eof);
    ASSERT_TRUE(odata != nullptr);
    auto outputTable = getTable(odata);
    ASSERT_TRUE(outputTable != nullptr);
    ASSERT_EQ(6, outputTable->getRowCount());
    ASSERT_EQ(12, outputTable->getColumnCount());
    compareMultiColumn<autil::MultiChar, string>(
        outputTable.get(),
        "aid",
        {{"0"}, {"0", "not_found", "2"}, {"3"}, {"4", "5"}, {"5", "4"}, {"10"}});
    compareSingleColumn<int32_t>(outputTable.get(), "attr", {1, 1, 2, 3, 2, 10000});
    compareStringColumn(
        outputTable.get(), "path", {"-/a", "-/a/b", "-/a/b/c", "-/a/d/c", "-/a/c", "-hehehe"});
    compareMultiColumn<int32_t>(
        outputTable.get(), "attr0", {{1}, {1, 0, 2}, {2}, {3, 3}, {3, 3}, {0}});
    compareMultiColumn<autil::MultiChar, string>(
        outputTable.get(), "aid0", {{"0"}, {"0", "", "2"}, {"3"}, {"4", "5"}, {"5", "4"}, {""}});
    compareMultiColumn<autil::MultiChar, string>(
        outputTable.get(),
        "path0",
        {{"/a"}, {"/a", "", "/a/b"}, {"/a/b/c"}, {"/a/d/c", "/a/c/c"}, {"/a/c/c", "/a/d/c"}, {""}});
}

} // namespace sql
