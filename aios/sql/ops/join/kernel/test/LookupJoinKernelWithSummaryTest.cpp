#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <limits>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/data/TableData.h"
#include "sql/ops/test/OpTestBase.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "turing_ops_util/variant/Tracer.h"
#include "unittest/unittest.h"

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class LookupJoinKernelWithSummaryTest : public OpTestBase {
public:
    LookupJoinKernelWithSummaryTest();
    ~LookupJoinKernelWithSummaryTest();

public:
    void setUp() override {
        _needBuildIndex = true;
        _needExprResource = true;
        _equiCondition = "{\"op\":\"=\", \"type\":\"OTHER\", \"params\":[\"$aid\", \"$aid0\"]}";
        _partialEquiCondition = "{\"op\":\"AND\",\"type\":\"OTHER\",\"params\":[{\"op\":\"=\","
                                "\"type\":\"OTHER\",\"params\":[\"$aid\",\"$aid0\"]},{\"op\":\"=\","
                                "\"type\":\"OTHER\",\"params\":[\"$path\",\"$path0\"]}]}";
        _buildNodeWithCompareCondition
            = R"({"table_name":"acl", "db_name":"default", "catalog_name":"default",
               "table_meta": {"field_meta":[
               {"index_type":"primarykey64", "index_name":"aid", "field_name":"$aid","field_type":"int32"},
               {"index_type":"STRING", "index_name":"path", "field_name": "$path", "field_type": "string"},
               {"index_type":"NUMBER", "index_name":"attr", "field_name": "$attr", "field_type": "int32"}]},
               "output_fields_internal":["$aid", "$path", "$group"],
               "use_nest_table":false, "table_type":"summary", "batch_size":1,
              "hash_type":"HASH", "used_fields":["$aid", "$path", "$group"],
              "hash_fields":["$aid"], "condition":
              {"op":">","type":"OTHER","params":["$aid",3]}})";
    }
    void tearDown() override {
        _attributeMap.clear();
    }

private:
    string _equiCondition;
    string _partialEquiCondition;
    string _buildNodeWithCompareCondition;

private:
    // summary table fields: aid, path, group
    // input table fields: aid, path
    void prepareAttribute(bool leftIsBuild,
                          string joinType,
                          string equiCondition,
                          string buildNode = "") {
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_is_build"] = leftIsBuild;
        _attributeMap["join_type"] = joinType;
        if (joinType == "SEMI" || joinType == "ANTI") {
            _attributeMap["semi_join_type"] = joinType;
        }
        if (leftIsBuild) {
            _attributeMap["left_input_fields"]
                = ParseJson(R"json(["$aid", "$path", "$group"])json");
            _attributeMap["left_table_meta"] = ParseJson(
                R"json({"field_meta":[{"index_type":"primarykey64", "index_name":"aid", "field_name": "$aid", "field_type": "int32"},
                                      {"index_type":"STRING", "index_name":"path", "field_name": "$path", "field_type": "string"},
                                      {"index_type":"NUMBER", "index_name":"attr", "field_name": "$attr", "field_type": "int32"}]})json");
            _attributeMap["right_input_fields"] = ParseJson(R"json(["$aid", "$path"])json");
        } else {
            _attributeMap["left_input_fields"] = ParseJson(R"json(["$aid", "$path"])json");
            _attributeMap["right_input_fields"]
                = ParseJson(R"json(["$aid", "$path", "$group"])json");
            _attributeMap["right_table_meta"] = ParseJson(
                R"json({"field_meta":[{"index_type":"primarykey64", "index_name":"aid", "field_name": "$aid", "field_type": "int32"},
                                      {"index_type":"STRING", "index_name":"path", "field_name": "$path", "field_type": "string"},
                                      {"index_type":"NUMBER", "index_name":"attr", "field_name": "$attr", "field_type": "int32"}]})json");
        }
        _attributeMap["system_field_num"] = Any(0);
        if (joinType == "SEMI" || joinType == "ANTI") {
            if (leftIsBuild) {
                _attributeMap["output_fields"]
                    = ParseJson(R"json(["$aid", "$path", "$group"])json");
                _attributeMap["output_fields_internal"]
                    = ParseJson(R"json(["$aid", "$path", "$group", "$aid0", "$path0"])json");
            } else {
                _attributeMap["output_fields"] = ParseJson(R"json(["$aid", "$path"])json");
                _attributeMap["output_fields_internal"]
                    = ParseJson(R"json(["$aid", "$path", "$aid0", "$path0", "$group0"])json");
            }
        } else {
            if (leftIsBuild) {
                _attributeMap["output_fields"]
                    = ParseJson(R"json(["$aid", "$path", "$group", "$aid0", "$path0"])json");
            } else {
                _attributeMap["output_fields"]
                    = ParseJson(R"json(["$aid", "$path", "$aid0", "$path0", "$group"])json");
            }
        }
        _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(equiCondition);
        if (buildNode.empty()) {
            _attributeMap["build_node"] = ParseJson(
                R"json({"table_name":"acl", "db_name":"default", "catalog_name":"default", "table_meta": {"field_meta":[{"index_type":"primarykey64", "index_name":"aid", "field_name":"$aid", "field_type":"int32"}, {"index_type":"STRING", "index_name":"path","field_name":"$path","field_type":"string"}, {"index_type":"NUMBER", "index_name":"attr", "field_name":"$attr", "field_type":"int32"}]}, "output_fields_internal":["$aid", "$path", "$group"], "use_nest_table":false, "table_type":"summary", "batch_size":1, "hash_type":"HASH", "used_fields":["$aid", "$path", "$group"], "hash_fields":["$aid"]})json");
        } else {
            _attributeMap["build_node"] = ParseJson(buildNode);
        }
    }

    void prepareInvertedTableData(string &tableName,
                                  string &fields,
                                  string &indexes,
                                  string &attributes,
                                  string &summarys,
                                  string &truncateProfileStr,
                                  string &docs,
                                  int64_t &ttl) override {
        _tableName = "acl";
        tableName = _tableName;
        fields = "aid:int64;path:string;group:string;attr:int32";
        indexes = "aid:primarykey64:aid;path:string:path;attr:number:attr";
        attributes = "aid;path;group;attr";
        summarys = "aid;path;group;attr";
        truncateProfileStr = "";
        docs = "cmd=add,aid=0,path=/a,attr=1,group=groupA;"
               "cmd=add,aid=1,path=/a/b,attr=1,group=groupA;"
               "cmd=add,aid=2,path=/a/b,attr=2,group=groupB;"
               "cmd=add,aid=3,path=/a/b/c,attr=2,group=groupB;"
               "cmd=add,aid=4,path=/a/d/c,attr=3,group=groupA;"
               "cmd=add,aid=5,path=/a/c/c,attr=1,group=groupA;"
               "cmd=add,aid=6,path=/a/c,attr=2,group=groupB;";
        ttl = numeric_limits<int64_t>::max();
    }
    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
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
            cout << i << ": " << name << " : " << column->toString(i) << endl;
            ASSERT_EQ(expect[i], column->toString(i));
        }
    }

public:
    MatchDocAllocatorPtr _allocator;
};

LookupJoinKernelWithSummaryTest::LookupJoinKernelWithSummaryTest() {}

LookupJoinKernelWithSummaryTest::~LookupJoinKernelWithSummaryTest() {}

TEST_F(LookupJoinKernelWithSummaryTest, testErrorKernel) {
    {
        prepareAttribute(true, "LEFT", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }
    {
        prepareAttribute(true, "ANTI", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_TRUE(testerPtr->hasError());
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testSingleValLeftJoinRight) {
    {
        prepareAttribute(false, "LEFT", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "6", "10"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expAid0 = {"0", "3", "6", "0"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid0, odata, "aid0"));
        vector<string> expPath = {"/a", "/a/b", "/a/c", "/b/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"/a", "/a/b/c", "/a/c", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expGroup = {"groupA", "groupB", "groupB", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
    }
    {
        prepareAttribute(false, "LEFT", _equiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"6", "0", "3", "10"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid, odata, "aid"));
        vector<string> expAid0 = {"6", "0", "0", "0"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expAid0, odata, "aid0"));
        vector<string> expPath = {"/a/c", "/a", "/a/b", "/b/c"};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath, odata, "path"));
        vector<string> expPath0 = {"/a/c", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expPath0, odata, "path0"));
        vector<string> expGroup = {"groupB", "", "", ""};
        ASSERT_NO_FATAL_FAILURE(checkOutput(expGroup, odata, "group"));
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testSingleValAntiJoinRight) {
    {
        prepareAttribute(false, "ANTI", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/b/c"};
        checkOutput(expPath, odata, "path");
    }
    {
        prepareAttribute(false, "ANTI", _equiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testSingleValInnerJoinLeft) {
    {
        prepareAttribute(true, "INNER", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "6"};
        checkOutput(expAid, odata, "aid");
        checkOutput(expAid, odata, "aid0");
        vector<string> expPath = {"/a", "/a/b/c", "/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a", "/a/b", "/a/c"};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupA", "groupB", "groupB"};
        checkOutput(expGroup, odata, "group");
    }
    {
        prepareAttribute(true, "INNER", _equiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"6"};
        checkOutput(expAid, odata, "aid");
        checkOutput(expAid, odata, "aid0");
        vector<string> expPath = {"/a/c"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testSingleValSemiJoinLeft) {
    {
        prepareAttribute(true, "SEMI", _equiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "6"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a", "/a/b/c", "/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expGroup = {"groupA", "groupB", "groupB"};
        checkOutput(expGroup, odata, "group");
    }
    {
        prepareAttribute(true, "SEMI", _equiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"6"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testPartialIndexLeftJoinRight) {
    {
        prepareAttribute(false, "LEFT", _partialEquiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "6", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expAid0 = {"0", "6", "0", "0"};
        checkOutput(expAid0, odata, "aid0");
        vector<string> expPath = {"/a", "/a/c", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a", "/a/c", "", ""};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupA", "groupB", "", ""};
        checkOutput(expGroup, odata, "group");
    }
    {
        prepareAttribute(false, "LEFT", _partialEquiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"6", "0", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expAid0 = {"6", "0", "0", "0"};
        checkOutput(expAid0, odata, "aid0");
        vector<string> expPath = {"/a/c", "/a", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a/c", "", "", ""};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupB", "", "", ""};
        checkOutput(expGroup, odata, "group");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testPartialIndexAntiJoinRight) {
    {
        prepareAttribute(false, "ANTI", _partialEquiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
    }
    {
        prepareAttribute(false, "ANTI", _partialEquiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "3", "10"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a", "/a/b", "/b/c"};
        checkOutput(expPath, odata, "path");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testPartialIndexInnerJoinRight) {
    {
        prepareAttribute(true, "INNER", _partialEquiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "6"};
        checkOutput(expAid, odata, "aid");
        checkOutput(expAid, odata, "aid0");
        vector<string> expPath = {"/a", "/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expPath0 = {"/a", "/a/c"};
        checkOutput(expPath0, odata, "path0");
        vector<string> expGroup = {"groupA", "groupB"};
        checkOutput(expGroup, odata, "group");
    }
    {
        prepareAttribute(true, "INNER", _partialEquiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"6"};
        checkOutput(expAid, odata, "aid");
        checkOutput(expAid, odata, "aid0");
        vector<string> expPath = {"/a/c"};
        checkOutput(expPath, odata, "path");
        checkOutput(expPath, odata, "path0");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }
}

TEST_F(LookupJoinKernelWithSummaryTest, testPartialIndexSemiJoinLeft) {
    {
        prepareAttribute(true, "SEMI", _partialEquiCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"0", "6"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a", "/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expGroup = {"groupA", "groupB"};
        checkOutput(expGroup, odata, "group");
    }
    {
        prepareAttribute(true, "SEMI", _partialEquiCondition, _buildNodeWithCompareCondition);
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
                _allocator, leftDocs, "aid", {0, 3, 6, 10}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator, leftDocs, "path", {"/a", "/a/b", "/a/c", "/b/c"}));
            auto leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));
        }

        TablePtr outputTable;
        ASSERT_NO_FATAL_FAILURE(asyncRunKernelToEof(*testerPtr, outputTable));
        ASSERT_NE(nullptr, outputTable);
        auto odata = std::make_shared<TableData>(std::move(outputTable));

        vector<string> expAid = {"6"};
        checkOutput(expAid, odata, "aid");
        vector<string> expPath = {"/a/c"};
        checkOutput(expPath, odata, "path");
        vector<string> expGroup = {"groupB"};
        checkOutput(expGroup, odata, "group");
    }
}

} // namespace sql
