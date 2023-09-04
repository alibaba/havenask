#include "sql/ops/join/JoinKernelBase.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "autil/HashUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/ops/join/JoinBase.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil;
using namespace table;
using namespace autil::legacy;
using namespace autil::legacy::json;

namespace sql {

class JoinKernelBaseTest : public OpTestBase {
public:
    JoinKernelBaseTest();
    ~JoinKernelBaseTest();

public:
    void setUp() override {
        prepareAttribute();
    }
    void tearDown() override {
        _attributeMap.clear();
    }

private:
    void prepareAttribute() {
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$uid", "$cid", "$cid0", "$group_name0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$cid0", "$cid"]})json");
    }
    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.JoinKernelBase");
        builder.input("input0");
        builder.input("input1");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        return builder.build();
    }
    void checkColumn(const TablePtr &table, string columnName, BuiltinType type, bool isMulti) {
        auto tableColumn = table->getColumn(columnName);
        ASSERT_TRUE(tableColumn != NULL);
        auto schema = tableColumn->getColumnSchema();
        ASSERT_TRUE(schema != NULL);
        auto vt = schema->getType();
        ASSERT_EQ(type, vt.getBuiltinType());
        ASSERT_EQ(isMulti, vt.isMultiValue());
    }

    void checkHashValuesOffset(const JoinKernelBase::HashValues &hashValues,
                               JoinKernelBase::HashValues expectValues) {
        ASSERT_EQ(hashValues.size(), expectValues.size());
        for (size_t i = 0; i < hashValues.size(); i++) {
            EXPECT_EQ(expectValues[i], hashValues[i]) << i;
        }
    }
    void checkHashValuesOffsetWithHash(const JoinKernelBase::HashValues &hashValues,
                                       JoinKernelBase::HashValues expectValues) {
        ASSERT_EQ(hashValues.size(), expectValues.size());
        for (size_t i = 0; i < expectValues.size(); i++) {
            auto hashVal = expectValues[i];
            hashVal.second = autil::HashUtil::calculateHashValue<int32_t>(hashVal.second);
            ASSERT_EQ(hashVal, hashValues[i]) << i;
        }
    }
};

JoinKernelBaseTest::JoinKernelBaseTest() {}

JoinKernelBaseTest::~JoinKernelBaseTest() {}

TEST_F(JoinKernelBaseTest, testJoinKeyInSameTable) {
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(JoinKernelBaseTest, testInputOutputNotSame) {
    _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
    _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
    _attributeMap["output_fields"] = ParseJson(R"json(["$uid", "$cid0", "$group_name0"])json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(JoinKernelBaseTest, testInputOutputNotSame2) {
    _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
    _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$uid","$cid", "$cid0","$group_name0", "$group_name1"])json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(JoinKernelBaseTest, testSystemFieldNotZero) {
    _attributeMap["system_field_num"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(JoinKernelBaseTest, testJoinMultiFieldFailed) {
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", " + anyUdfStr + "]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(JoinKernelBaseTest, testConvertFields) {
    {
        JoinKernelBase joinBase;
        joinBase._systemFieldNum = 1;
        ASSERT_FALSE(joinBase.convertFields());
    }
    {
        JoinKernelBase joinBase;
        ASSERT_TRUE(joinBase.convertFields());
    }
    {
        JoinKernelBase joinBase;
        joinBase._joinBaseParam._leftInputFields = {"1", "2"};
        ASSERT_FALSE(joinBase.convertFields());
        joinBase._joinBaseParam._rightInputFields = {"3"};
        joinBase._outputFields = {"1", "2", "30"};
        ASSERT_TRUE(joinBase.convertFields());
        ASSERT_EQ("1", joinBase._output2InputMap["1"].first);
        ASSERT_TRUE(joinBase._output2InputMap["1"].second);
        ASSERT_EQ("2", joinBase._output2InputMap["2"].first);
        ASSERT_TRUE(joinBase._output2InputMap["2"].second);
        ASSERT_EQ("3", joinBase._output2InputMap["30"].first);
        ASSERT_FALSE(joinBase._output2InputMap["30"].second);
    }
}

TEST_F(JoinKernelBaseTest, testCombineHashValues) {
    {
        JoinKernelBase::HashValues left = {{1, 10}, {2, 22}, {3, 33}};
        JoinKernelBase::HashValues right = {{1, 100}, {2, 122}, {3, 133}};
        JoinKernelBase joinBase;
        joinBase.combineHashValues(left, right);
        ASSERT_EQ(3, right.size());
    }
    {
        JoinKernelBase::HashValues left = {};
        JoinKernelBase::HashValues right = {{1, 100}, {2, 122}, {3, 133}};
        JoinKernelBase joinBase;
        joinBase.combineHashValues(left, right);
        ASSERT_EQ(0, right.size());
    }
    {
        JoinKernelBase::HashValues left = {{1, 10}, {2, 22}, {3, 33}};
        JoinKernelBase::HashValues right = {};
        JoinKernelBase joinBase;
        joinBase.combineHashValues(left, right);
        ASSERT_EQ(0, right.size());
    }
    {
        JoinKernelBase::HashValues left = {{1, 10}, {3, 22}, {5, 33}};
        JoinKernelBase::HashValues right = {{2, 100}, {4, 122}, {6, 133}};
        JoinKernelBase joinBase;
        joinBase.combineHashValues(left, right);
        ASSERT_EQ(0, right.size());
    }
    {
        JoinKernelBase::HashValues left = {{1, 10}, {1, 22}, {1, 33}};
        JoinKernelBase::HashValues right = {{1, 100}, {1, 122}, {3, 133}};
        JoinKernelBase joinBase;
        joinBase.combineHashValues(left, right);
        ASSERT_EQ(6, right.size());
    }
    {
        JoinKernelBase::HashValues left = {{2, 10}};
        JoinKernelBase::HashValues right = {{1, 100}, {2, 22}, {3, 33}};
        JoinKernelBase joinBase;
        joinBase.combineHashValues(left, right);
        ASSERT_EQ(1, right.size());
    }
}

TEST_F(JoinKernelBaseTest, testCreateHashMap) {
    {
        JoinKernelBase base;
        base._leftJoinColumns = {"cid"};
        base._rightJoinColumns = {"cid"};
        MatchDocAllocatorPtr allocator;
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "cid", {"1111", "3333"}));
        TablePtr inputTable(new Table(leftDocs, allocator));
        ASSERT_TRUE(base.createHashMap(inputTable, 0, inputTable->getRowCount(), true));
        ASSERT_EQ(2, base._hashJoinMap.size());
    }
    {
        JoinKernelBase base;
        base._leftJoinColumns = {"muid", "cid", "mcid"};
        base._rightJoinColumns = {"muid", "cid", "mcid"};
        MatchDocAllocatorPtr allocator;
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "cid", {"1111", "3333"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            allocator, leftDocs, "muid", {{1, 3}, {2}}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            allocator, leftDocs, "mcid", {{1, 3, 4}, {2, 4, 5, 6}}));
        TablePtr inputTable(new Table(leftDocs, allocator));
        ASSERT_TRUE(base.createHashMap(inputTable, 0, inputTable->getRowCount(), true));
        ASSERT_EQ(10, base._hashJoinMap.size());
    }
    {
        JoinKernelBase base;
        base._leftJoinColumns = {"muid", "cid", "mcid"};
        base._rightJoinColumns = {"muid", "cid", "mcid"};
        MatchDocAllocatorPtr allocator;
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "cid", {"1111", "3333"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            allocator, leftDocs, "muid", {{1, 3}, {2}}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            allocator, leftDocs, "mcid", {{1, 3, 4}, {2, 4, 2, 2}}));
        TablePtr inputTable(new Table(leftDocs, allocator));
        ASSERT_TRUE(base.createHashMap(inputTable, 0, inputTable->getRowCount(), true));
        ASSERT_EQ(8, base._hashJoinMap.size());
    }
    { // multi value has empty
        JoinKernelBase base;
        base._leftJoinColumns = {"muid", "cid", "mcid"};
        base._rightJoinColumns = {"muid", "cid", "mcid"};
        MatchDocAllocatorPtr allocator;
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "cid", {"1111", "3333"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            allocator, leftDocs, "muid", {{1, 3}, {2}}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            allocator, leftDocs, "mcid", {{1, 3, 4}, {}}));
        TablePtr inputTable(new Table(leftDocs, allocator));
        ASSERT_TRUE(base.createHashMap(inputTable, 0, inputTable->getRowCount(), true));
        ASSERT_EQ(6, base._hashJoinMap.size());
    }
}

TEST_F(JoinKernelBaseTest, testGetColumnHashValues) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 3);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid2", {2, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
        allocator, leftDocs, "cid", {"1111", "3333", "1111"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1, 3}, {2}, {4}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "mcid", {{1, 3, 4}, {2, 4, 5, 6}, {}}));
    TablePtr inputTable(new Table(leftDocs, allocator));
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(
            base.getColumnHashValues(inputTable, 0, inputTable->getRowCount(), "uid", hashValues));
        ASSERT_FALSE(base._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(
            checkHashValuesOffsetWithHash(hashValues, {{0, 0}, {1, 1}, {2, 2}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getColumnHashValues(inputTable, 0, 1, "uid", hashValues));
        ASSERT_FALSE(base._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {{0, 0}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getColumnHashValues(inputTable, 1, 2, "uid", hashValues));
        ASSERT_FALSE(base._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {{1, 1}, {2, 2}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getColumnHashValues(inputTable, 2, 4, "uid", hashValues));
        ASSERT_FALSE(base._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {{2, 2}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getColumnHashValues(inputTable, 5, 4, "uid", hashValues));
        ASSERT_TRUE(base._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getColumnHashValues(inputTable, 0, 4, "mcid", hashValues));
        ASSERT_FALSE(base._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(
            hashValues, {{0, 1}, {0, 3}, {0, 4}, {1, 2}, {1, 4}, {1, 5}, {1, 6}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getColumnHashValues(inputTable, 2, 1, "mcid", hashValues));
        ASSERT_TRUE(base._shouldClearTable);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(hashValues, {}));
    }
}

TEST_F(JoinKernelBaseTest, testGetHashValues) {
    MatchDocAllocatorPtr allocator;
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 3);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid", {0, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(allocator, leftDocs, "uid2", {2, 1, 0}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
        allocator, leftDocs, "cid", {"1111", "3333", "1111"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "muid", {{1, 3}, {2}, {4}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        allocator, leftDocs, "mcid", {{1, 3, 4}, {2, 4, 5, 6}, {}}));
    TablePtr inputTable(new Table(leftDocs, allocator));
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getHashValues(inputTable, 0, 3, {"uid"}, hashValues));
        ASSERT_NO_FATAL_FAILURE(
            checkHashValuesOffsetWithHash(hashValues, {{0, 0}, {1, 1}, {2, 2}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getHashValues(inputTable, 0, 3, {"uid2"}, hashValues));
        ASSERT_NO_FATAL_FAILURE(
            checkHashValuesOffsetWithHash(hashValues, {{0, 2}, {1, 1}, {2, 0}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getHashValues(inputTable, 0, 3, {"uid", "uid2"}, hashValues));
        size_t v1 = autil::HashUtil::calculateHashValue(2);
        autil::HashUtil::combineHash(v1, 0);
        size_t v2 = autil::HashUtil::calculateHashValue(1);
        autil::HashUtil::combineHash(v2, 1);
        size_t v3 = autil::HashUtil::calculateHashValue(0);
        autil::HashUtil::combineHash(v3, 2);
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffset(hashValues, {{0, v1}, {1, v2}, {2, v3}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getHashValues(inputTable, 0, 3, {"cid"}, hashValues));
        ASSERT_NO_FATAL_FAILURE(
            checkHashValuesOffset(hashValues,
                                  {{0, autil::HashUtil::calculateHashValue(string("1111"))},
                                   {1, autil::HashUtil::calculateHashValue(string("3333"))},
                                   {2, autil::HashUtil::calculateHashValue(string("1111"))}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getHashValues(inputTable, 0, 3, {"mcid"}, hashValues));
        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffsetWithHash(
            hashValues, {{0, 1}, {0, 3}, {0, 4}, {1, 2}, {1, 4}, {1, 5}, {1, 6}}));
    }
    {
        JoinKernelBase base;
        JoinKernelBase::HashValues hashValues;
        ASSERT_TRUE(base.getHashValues(inputTable, 0, 3, {"muid", "mcid"}, hashValues));
        size_t v1 = autil::HashUtil::calculateHashValue(1);
        autil::HashUtil::combineHash(v1, 1);
        size_t v2 = autil::HashUtil::calculateHashValue(1);
        autil::HashUtil::combineHash(v2, 3);
        size_t v3 = autil::HashUtil::calculateHashValue(3);
        autil::HashUtil::combineHash(v3, 1);
        size_t v4 = autil::HashUtil::calculateHashValue(3);
        autil::HashUtil::combineHash(v4, 3);
        size_t v5 = autil::HashUtil::calculateHashValue(4);
        autil::HashUtil::combineHash(v5, 1);
        size_t v6 = autil::HashUtil::calculateHashValue(4);
        autil::HashUtil::combineHash(v6, 3);
        size_t v7 = autil::HashUtil::calculateHashValue(2);
        autil::HashUtil::combineHash(v7, 2);
        size_t v8 = autil::HashUtil::calculateHashValue(4);
        autil::HashUtil::combineHash(v8, 2);
        size_t v9 = autil::HashUtil::calculateHashValue(5);
        autil::HashUtil::combineHash(v9, 2);
        size_t v10 = autil::HashUtil::calculateHashValue(6);
        autil::HashUtil::combineHash(v10, 2);

        ASSERT_NO_FATAL_FAILURE(checkHashValuesOffset(hashValues,
                                                      {{0, v1},
                                                       {0, v2},
                                                       {0, v3},
                                                       {0, v4},
                                                       {0, v5},
                                                       {0, v6},
                                                       {1, v7},
                                                       {1, v8},
                                                       {1, v9},
                                                       {1, v10}}));
    }
}

TEST_F(JoinKernelBaseTest, testCanTruncate) {
    ASSERT_FALSE(JoinKernelBase::canTruncate(1, 0));
    ASSERT_TRUE(JoinKernelBase::canTruncate(1, 1));
    ASSERT_FALSE(JoinKernelBase::canTruncate(1, 2));
}

TEST_F(JoinKernelBaseTest, testPatchHintInfo) {
    {
        JoinKernelBase joinKernel;
        map<string, string> hintsMap = {};
        joinKernel.patchHintInfo(hintsMap);
        ASSERT_EQ(100000, joinKernel._batchSize);
        ASSERT_EQ(0, joinKernel._truncateThreshold);
        ASSERT_EQ(0, joinKernel._joinBaseParam._defaultValue.size());
    }
    {
        JoinKernelBase joinKernel;
        map<string, string> hintsMap
            = {{"batchSize", "1"}, {"truncateThreshold", "2"}, {"defaultValue", "a:1,b:2"}};
        joinKernel.patchHintInfo(hintsMap);
        ASSERT_EQ(1, joinKernel._batchSize);
        ASSERT_EQ(2, joinKernel._truncateThreshold);
        ASSERT_EQ(2, joinKernel._joinBaseParam._defaultValue.size());
    }
}

} // namespace sql
