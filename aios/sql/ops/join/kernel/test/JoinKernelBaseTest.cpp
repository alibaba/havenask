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

TEST_F(JoinKernelBaseTest, testCreateHashMap) {
    {
        JoinKernelBase base;
        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes<JoinInfoCollectorR>(base._joinInfoR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes<HashJoinMapR>(base._hashJoinMapR));
        JoinBaseParamR joinParamR;
        base._joinParamR = &joinParamR;
        base._joinParamR->_leftJoinColumns = {"cid"};
        base._joinParamR->_rightJoinColumns = {"cid"};
        MatchDocAllocatorPtr allocator;
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(allocator, leftDocs, "cid", {"1111", "3333"}));
        TablePtr inputTable = Table::fromMatchDocs(leftDocs, allocator);
        ASSERT_TRUE(base.createHashMap(inputTable, 0, inputTable->getRowCount(), true));
        ASSERT_EQ(2, base._hashJoinMapR->_hashJoinMap.size());
    }
    {
        JoinKernelBase base;
        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes<JoinInfoCollectorR>(base._joinInfoR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes<HashJoinMapR>(base._hashJoinMapR));
        JoinBaseParamR joinParamR;
        base._joinParamR = &joinParamR;
        base._joinParamR->_leftJoinColumns = {"muid", "cid", "mcid"};
        base._joinParamR->_rightJoinColumns = {"muid", "cid", "mcid"};
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
        TablePtr inputTable = Table::fromMatchDocs(leftDocs, allocator);
        ASSERT_TRUE(base.createHashMap(inputTable, 0, inputTable->getRowCount(), true));
        ASSERT_EQ(10, base._hashJoinMapR->_hashJoinMap.size());
    }
    {
        JoinKernelBase base;
        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes<JoinInfoCollectorR>(base._joinInfoR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes<HashJoinMapR>(base._hashJoinMapR));
        JoinBaseParamR joinParamR;
        base._joinParamR = &joinParamR;
        base._joinParamR->_leftJoinColumns = {"muid", "cid", "mcid"};
        base._joinParamR->_rightJoinColumns = {"muid", "cid", "mcid"};
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
        TablePtr inputTable = Table::fromMatchDocs(leftDocs, allocator);
        ASSERT_TRUE(base.createHashMap(inputTable, 0, inputTable->getRowCount(), true));
        ASSERT_EQ(8, base._hashJoinMapR->_hashJoinMap.size());
    }
    { // multi value has empty
        JoinKernelBase base;
        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes<JoinInfoCollectorR>(base._joinInfoR));
        ASSERT_TRUE(naviRHelper->getOrCreateRes<HashJoinMapR>(base._hashJoinMapR));
        JoinBaseParamR joinParamR;
        base._joinParamR = &joinParamR;
        base._joinParamR->_leftJoinColumns = {"muid", "cid", "mcid"};
        base._joinParamR->_rightJoinColumns = {"muid", "cid", "mcid"};
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
        TablePtr inputTable = Table::fromMatchDocs(leftDocs, allocator);
        ASSERT_TRUE(base.createHashMap(inputTable, 0, inputTable->getRowCount(), true));
        ASSERT_EQ(6, base._hashJoinMapR->_hashJoinMap.size());
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
        JoinBaseParamR joinPR;
        joinKernel._joinParamR = &joinPR;
        map<string, string> hintsMap = {};
        joinKernel.patchHintInfo(hintsMap);
        ASSERT_EQ(100000, joinKernel._batchSize);
        ASSERT_EQ(0, joinKernel._truncateThreshold);
    }
    {
        JoinKernelBase joinKernel;
        JoinBaseParamR joinPR;
        joinKernel._joinParamR = &joinPR;
        map<string, string> hintsMap = {{"batchSize", "1"}, {"truncateThreshold", "2"}};
        joinKernel.patchHintInfo(hintsMap);
        ASSERT_EQ(1, joinKernel._batchSize);
        ASSERT_EQ(2, joinKernel._truncateThreshold);
    }
}

} // namespace sql
