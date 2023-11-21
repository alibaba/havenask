#include "sql/ops/join/HashJoinKernel.h"

#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <stddef.h>
#include <string>
#include <vector>

#include "autil/MultiValueCreator.h"
#include "autil/MultiValueType.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/common.h"
#include "navi/engine/Data.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
#include "sql/data/TableData.h"
#include "sql/ops/join/JoinKernelBase.h"
#include "sql/ops/test/OpTestBase.h"
#include "suez/turing/navi/QueryMemPoolR.h"
#include "table/Table.h"
#include "table/TableUtil.h"
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

class HashJoinKernelTest : public OpTestBase {
public:
    HashJoinKernelTest();
    ~HashJoinKernelTest();

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
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$uid", "$cid", "$cid0", "$group_name0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$cid0", "$cid"]})json");
    }
    void prepareAttributeSemiJoin() {
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["semi_join_type"] = string("SEMI");
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["output_fields_internal"] = ParseJson(R"json(["$uid", "$cid", "$cid0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$cid0", "$cid"]})json");
    }

    void prepareAttributeAntiJoin() {
        _attributeMap["join_type"] = string("ANTI");
        _attributeMap["semi_join_type"] = string("ANTI");
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["output_fields_internal"] = ParseJson(R"json(["$uid", "$cid", "$cid0"])json");
        _attributeMap["equi_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$cid0", "$cid"]})json");
    }

    void semiBuildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.HashJoinKernel");
        builder.input("input0");
        builder.input("input1");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        _testerPtr = builder.build();
        ASSERT_TRUE(_testerPtr.get());
        ASSERT_FALSE(_testerPtr->hasError());
    }

    void prepareTable1(const vector<int32_t> &uid,
                       const vector<int32_t> &cid,
                       bool eof,
                       bool isLeftTable = true) {
        std::string port = isLeftTable ? "input0" : "input1";
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, uid.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", uid));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "cid", cid));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(_testerPtr->setInput(port, leftTable, eof));
    }

    void prepareTable2(const vector<int32_t> &cid,
                       const vector<string> &group,
                       bool eof,
                       bool isLeftTable = false) {
        std::string port = isLeftTable ? "input0" : "input1";
        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, cid.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, rightDocs, "cid", cid));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, rightDocs, "group_name", group));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(_testerPtr->setInput(port, rightTable, eof));
    }

    void prepareTable3(const vector<int32_t> &uid,
                       const vector<vector<int32_t>> &cid,
                       bool eof,
                       bool isLeftTable = true) {
        std::string port = isLeftTable ? "input0" : "input1";
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, uid.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", uid));
        vector<MultiValueType<int32_t>> cidValue;
        for (auto &id : cid) {
            char *buf1
                = MultiValueCreator::createMultiValueBuffer(id.data(), id.size(), _poolPtr.get());
            cidValue.emplace_back(buf1);
        }
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<MultiValueType<int32_t>>(
            _allocator, leftDocs, "cid", cidValue));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(_testerPtr->setInput(port, leftTable, eof));
    }

    void checkNoOutput() {
        if (_testerPtr->compute()) {
            ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
            DataPtr odata;
            bool eof;
            ASSERT_FALSE(_testerPtr->getOutput("output0", odata, eof));
        }
    }

    void checkOutputEof(bool isEof = true) {
        ASSERT_TRUE(_testerPtr->compute());
        ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
        DataPtr odata;
        bool eof;
        ASSERT_TRUE(_testerPtr->getOutput("output0", odata, eof));
        ASSERT_EQ(eof, isEof);
    }

    void checkSemiOutput(const vector<int32_t> &uid,
                         const vector<int32_t> &cid,
                         bool isEof,
                         bool hasOutput = true) {
        ASSERT_TRUE(_testerPtr->compute());
        ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

        TablePtr outputTable;
        bool eof = false;
        if (hasOutput == false) {
            return;
        }
        ASSERT_NO_FATAL_FAILURE(getOutputTable(_testerPtr, outputTable, eof));
        ASSERT_EQ(isEof, eof);
        ASSERT_EQ(cid.size(), outputTable->getRowCount()) << TableUtil::toString(outputTable);
        ASSERT_EQ(2, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", uid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", cid));
    }
    void checkOutput(const vector<int32_t> &uid,
                     const vector<int32_t> &cid,
                     const vector<int32_t> &cid0,
                     const vector<string> &group,
                     bool isEof,
                     bool hasOutput = true) {
        ASSERT_TRUE(_testerPtr->compute());
        ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

        TablePtr outputTable;
        bool eof = false;
        if (hasOutput == false) {
            return;
        }
        ASSERT_NO_FATAL_FAILURE(getOutputTable(_testerPtr, outputTable, eof));
        ASSERT_EQ(isEof, eof);
        ASSERT_EQ(cid.size(), outputTable->getRowCount()) << TableUtil::toString(outputTable);
        ASSERT_EQ(4, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", uid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", cid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", cid0));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", group));
    }
    void getOutputTable(KernelTesterPtr testerPtr, TablePtr &outputTable, bool &eof) {
        DataPtr odata;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        outputTable = getTable(odata);
        ASSERT_TRUE(outputTable != NULL);
    }

    KernelTesterPtr buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.HashJoinKernel");
        builder.input("input0");
        builder.input("input1");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        _testerPtr = builder.build();
        return _testerPtr;
    }

public:
    MatchDocAllocatorPtr _allocator;

private:
    KernelTesterPtr _testerPtr;
};

HashJoinKernelTest::HashJoinKernelTest() {}

HashJoinKernelTest::~HashJoinKernelTest() {}

TEST_F(HashJoinKernelTest, testSimpleProcess) {
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "cid", {1, 3}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, rightDocs, "cid", {0, 1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator,
            rightDocs,
            "group_name",
            {"groupA", "groupB", "groupC", "groupD", "groupE"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {1, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {1, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupB", "groupD"}));
}

TEST_F(HashJoinKernelTest, testReuseInput) {
    // reuse is empty
    {
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        DataPtr leftTable, rightTable;
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
                _allocator, leftDocs, "uid", {0, 0}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
                _allocator, leftDocs, "cid", {1, 3}));
            leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

            vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
                _allocator, rightDocs, "cid", {0, 1, 2, 3, 4}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator,
                rightDocs,
                "group_name",
                {"groupA", "groupB", "groupC", "groupD", "groupE"}));
            rightTable = createTable(_allocator, rightDocs);
            ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
        }
        ASSERT_TRUE(testerPtr->compute());
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        TablePtr outputTable;
        bool eof = false;
        getOutputTable(testerPtr, outputTable, eof);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, outputTable->getRowCount());
        ASSERT_EQ(4, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {1, 3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {1, 3}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "group_name0", {"groupB", "groupD"}));

        // check input
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(getTable(leftTable), "cid", {1, 3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(getTable(leftTable), "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(getTable(rightTable), "cid", {}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(getTable(rightTable), "group_name", {}));
    }

    // reuse inpute
    {
        _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
        auto testerPtr = buildTester(KernelTesterBuilder());
        ASSERT_TRUE(testerPtr.get());
        ASSERT_FALSE(testerPtr->hasError());
        DataPtr leftTable, rightTable;
        {
            vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
                _allocator, leftDocs, "uid", {0, 0}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
                _allocator, leftDocs, "cid", {1, 3}));
            leftTable = createTable(_allocator, leftDocs);
            ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

            vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
                _allocator, rightDocs, "cid", {0, 1, 2, 3, 4}));
            ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
                _allocator,
                rightDocs,
                "group_name",
                {"groupA", "groupB", "groupC", "groupD", "groupE"}));
            rightTable = createTable(_allocator, rightDocs);
            ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
        }
        ASSERT_TRUE(testerPtr->compute());
        ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

        TablePtr outputTable;
        bool eof = false;
        getOutputTable(testerPtr, outputTable, eof);
        ASSERT_TRUE(eof);
        ASSERT_EQ(2, outputTable->getRowCount());
        ASSERT_EQ(4, outputTable->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {1, 3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {1, 3}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(outputTable, "group_name0", {"groupB", "groupD"}));

        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(getTable(leftTable), "cid", {1, 3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(getTable(leftTable), "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<int32_t>(getTable(rightTable), "cid", {0, 1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(getTable(rightTable),
                              "group_name",
                              {"groupA", "groupB", "groupC", "groupD", "groupE"}));
    }
}

TEST_F(HashJoinKernelTest, testSemiJoinLeftFullTable) {
    prepareAttributeSemiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3, 4}, {"", "", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 2, 0}, {1, 2, 3}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoinRightFullTableBatchInput) {
    prepareAttributeSemiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3}, {"", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 0, 2}, {1, 3, 2}, false));
    ASSERT_TRUE(_testerPtr->setInputEof("input0"));
    ASSERT_NO_FATAL_FAILURE(checkOutputEof(true));
}

TEST_F(HashJoinKernelTest, testSemiJoinRightFullTable) {
    prepareAttributeSemiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3}, {"", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 0, 2}, {1, 3, 2}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoin_FilterLeftFullTable) {
    prepareAttributeSemiJoin();
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":"!=", "type":"OTHER", "params":["$uid", "$cid0"]})json");
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3, 4}, {"", "", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 0}, {1, 3}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoin_FilterRightFullTable) {
    prepareAttributeSemiJoin();
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":"!=", "type":"OTHER", "params":["$uid", "$cid0"]})json");
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3}, {"", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 0}, {1, 3}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoin_FilterLeftFullTable_Batch) {
    prepareAttributeSemiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0}, {1, 3}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1}, {"", ""}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable1({1, 2, 3}, {5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({2, 3, 4}, {"", "", ""}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 2, 0}, {1, 2, 3}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable2({5, 1}, {"", ""}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({1}, {5}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoin_FilterRightFullTable_Batch) {
    prepareAttributeSemiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0}, {1, 3}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3}, {"", "", "", ""}, true));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable1({1, 2, 3}, {5, 2, 10}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 0, 2}, {1, 3, 2}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable1({10, 11}, {0, 7}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10}, {0}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoinLeftFullTableBatchOutput) {
    prepareAttributeSemiJoin();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3, 4}, {"", "", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0}, {1}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({2}, {2}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0}, {3}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoinRightFullTableBatchOutput) {
    prepareAttributeSemiJoin();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3}, {"", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0}, {1}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0}, {3}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({2}, {2}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoin_FilterRightFullTableBatchInOut) {
    prepareAttributeSemiJoin();
    _attributeMap["batch_size"] = Any(2);
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":"!=", "type":"OTHER", "params":["$uid", "$cid0"]})json");
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2}, {1, 3, 5, 2}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({1, 2, 3}, {"", "", ""}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({0, 0}, {1, 3}, false));
    ASSERT_TRUE(_testerPtr->setInput("input0", nullptr, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({3, 4, 5, 6}, {1, 2, 9, 9}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({3, 4}, {1, 2}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, true));
}

TEST_F(HashJoinKernelTest, testSemiJoinMultiFieldWithEmpty) {
    prepareAttributeSemiJoin();
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid0\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable3({0, 1}, {{0, 1}, {}}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, true));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(!eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(outputTable, "cid", {{0, 1}}));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(0, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
}

TEST_F(HashJoinKernelTest, testSemiJoinMultiFieldWithAllEmpty) {
    prepareAttributeSemiJoin();
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid0\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable3({0, 1}, {{}, {}}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, true));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(0, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
}

TEST_F(HashJoinKernelTest, testAntiJoinLeftFullTable) {
    prepareAttributeAntiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3, 4}, {"", "", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({1, 3}, {5, 10}, true));
}

TEST_F(HashJoinKernelTest, testAntiJoinRightFullTable) {
    prepareAttributeAntiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3}, {"", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({1, 3}, {5, 10}, true));
}

TEST_F(HashJoinKernelTest, testAntiJoin_FilterLeftFullTable) {
    prepareAttributeAntiJoin();
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":"!=", "type":"OTHER", "params":["$uid", "$cid0"]})json");
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3, 4}, {"", "", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({1, 2, 3}, {5, 2, 10}, true));
}

TEST_F(HashJoinKernelTest, testAntiJoin_FilterRightFullTable) {
    prepareAttributeAntiJoin();
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":"!=", "type":"OTHER", "params":["$uid", "$cid0"]})json");
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2, 3}, {1, 3, 5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3}, {"", "", "", ""}, true));

    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({1, 2, 3}, {5, 2, 10}, true));
}

TEST_F(HashJoinKernelTest, testAntiJoin_FilterLeftFullTable_Batch) {
    prepareAttributeAntiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0}, {1, 3}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1}, {"", ""}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable1({1, 2, 3}, {5, 2, 10}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({2, 3, 4}, {"", "", ""}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable2({5, 1}, {"", ""}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, false));
    ASSERT_TRUE(_testerPtr->setInputEof("input1"));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({3}, {10}, true));
}

TEST_F(HashJoinKernelTest, testAntiJoin_FilterRightFullTable_Batch) {
    prepareAttributeAntiJoin();
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0}, {1, 3}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0, 1, 2, 3}, {"", "", "", ""}, true));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable1({1, 2, 3}, {5, 2, 10}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({1, 3}, {5, 10}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable1({10, 11}, {0, 7}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({11}, {7}, true));
}

TEST_F(HashJoinKernelTest, testAntiJoin_FilterRightFullTableBatchInOut) {
    prepareAttributeAntiJoin();
    _attributeMap["batch_size"] = Any(2);
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":"!=", "type":"OTHER", "params":["$uid", "$cid0"]})json");
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0, 1, 2}, {1, 3, 5, 2}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({1, 2, 3}, {"", "", ""}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, false));
    ASSERT_TRUE(_testerPtr->setInput("input0", nullptr, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({1, 2}, {5, 2}, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({3, 4, 5, 6}, {1, 2, 9, 9}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({5, 6}, {9, 9}, true));
}

TEST_F(HashJoinKernelTest, testAntiJoinMultiFieldWithEmpty) {
    prepareAttributeSemiJoin();
    _attributeMap["join_type"] = string("ANTI");
    _attributeMap["semi_join_type"] = string("ANTI");
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid0\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable3({0, 1}, {{0, 1}, {}}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, true));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(!eof);
    ASSERT_EQ(0, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(outputTable, "cid", {{}}));
}

TEST_F(HashJoinKernelTest, testAntiJoinMultiFieldWithAllEmpty) {
    prepareAttributeSemiJoin();
    _attributeMap["join_type"] = string("ANTI");
    _attributeMap["semi_join_type"] = string("ANTI");
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid0\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable3({0, 1}, {{}, {}}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, true));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<int32_t>(outputTable, "cid", {{}, {}}));
}

TEST_F(HashJoinKernelTest, testJoinWithFilter) {
    _attributeMap["is_equi_join"] = true;
    _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid", "$group_name"])json");
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$uid", "$cid", "$group_name", "$cid0", "$group_name0"])json");
    _attributeMap["remaining_condition"] = ParseJson(
        R"json({"op":"!=", "type":"OTHER", "params":["$group_name0", "$group_name"]})json");
    _attributeMap["equi_condition"]
        = ParseJson(R"json({"op":"=", "type":"OTHER", "params":["$cid0", "$cid"]})json");

    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", {0, 1, 2}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "cid", {1, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, leftDocs, "group_name", {"groupB", "groupB", "groupB"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, rightDocs, "cid", {0, 1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator,
            rightDocs,
            "group_name",
            {"groupA", "groupB", "groupC", "groupD", "groupB"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(5, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name", {"groupB"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupD"}));
}

TEST_F(HashJoinKernelTest, testNotJoinKey) {
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson("{}");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(HashJoinKernelTest, testJoinMultiField) {
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid0\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", {0, 1}));
        int32_t dataArray1[] = {0, 1};
        char *buf1 = MultiValueCreator::createMultiValueBuffer(dataArray1, 2, _poolPtr.get());
        MultiValueType<int32_t> multiValue1(buf1);
        int32_t dataArray2[] = {3, 4};
        char *buf2 = MultiValueCreator::createMultiValueBuffer(dataArray2, 2, _poolPtr.get());
        MultiValueType<int32_t> multiValue2(buf2);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<MultiValueType<int32_t>>(
            _allocator, leftDocs, "cid", {multiValue1, multiValue2}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, rightDocs, "cid", {0, 1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, rightDocs, "group_name", {"groupA", "groupB", "groupC", "groupD"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0, 0, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {0, 1, 3}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputColumn(outputTable, "group_name0", {"groupA", "groupB", "groupD"}));
}

TEST_F(HashJoinKernelTest, testJoinMultiFieldWithEmpty) {
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid0\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", {0, 1}));
        int32_t dataArray1[] = {0, 1};
        char *buf1 = MultiValueCreator::createMultiValueBuffer(dataArray1, 2, _poolPtr.get());
        MultiValueType<int32_t> multiValue1(buf1);
        int32_t dataArray2[] = {};
        char *buf2 = MultiValueCreator::createMultiValueBuffer(dataArray2, 0, _poolPtr.get());
        MultiValueType<int32_t> multiValue2(buf2);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<MultiValueType<int32_t>>(
            _allocator, leftDocs, "cid", {multiValue1, multiValue2}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 1);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, rightDocs, "cid", {0}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, rightDocs, "group_name", {"groupA"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(!eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupA"}));

    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_EQ(0, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_TRUE(eof);
}

TEST_F(HashJoinKernelTest, testJoinMultiFieldFailed) {
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", " + anyUdfStr + "]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_TRUE(testerPtr->hasError());
}

TEST_F(HashJoinKernelTest, testExchangeTable) {
    _attributeMap["left_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
    _attributeMap["right_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$cid", "$group_name", "$uid0", "$cid0"])json");
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, leftDocs, "cid", {0, 1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator,
            leftDocs,
            "group_name",
            {"groupA", "groupB", "groupC", "groupD", "groupE"}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, rightDocs, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, rightDocs, "cid", {1, 3}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {1, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid0", {0, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {1, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name", {"groupB", "groupD"}));
}

TEST_F(HashJoinKernelTest, testBatchInput) {
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    TablePtr outputTable;
    bool eof = false;
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "cid", {1, 3}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 1);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, rightDocs, "cid", {0}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, rightDocs, "group_name", {"groupA"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    DataPtr odata;
    ASSERT_FALSE(testerPtr->getOutput("output0", odata, eof));
    {
        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, rightDocs, "cid", {1, 2}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, rightDocs, "group_name", {"groupB", "groupC"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupB"}));
    {
        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, rightDocs, "cid", {3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, rightDocs, "group_name", {"groupD", "groupE"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupD"}));
}

TEST_F(HashJoinKernelTest, testBatchOutput) {
    _attributeMap["batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "cid", {1, 3}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, rightDocs, "cid", {0, 1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator,
            rightDocs,
            "group_name",
            {"groupA", "groupB", "groupC", "groupD", "groupE"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    TablePtr outputTable;
    bool eof = false;
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupB"}));

    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupD"}));

    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(0, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {}));
}

TEST_F(HashJoinKernelTest, testInputExceedBufferLimit) {
    _attributeMap["buffer_limit_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());

    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "cid", {1, 3}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 5);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, rightDocs, "cid", {0, 1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator,
            rightDocs,
            "group_name",
            {"groupA", "groupB", "groupC", "groupD", "groupE"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_ABORT, testerPtr->getErrorCode());
    ASSERT_EQ("input buffers exceed limit, cannot make hash join", testerPtr->getErrorMessage());
}

TEST_F(HashJoinKernelTest, testBufferLimitBlockInput) {
    _attributeMap["buffer_limit_size"] = Any(2);
    _attributeMap["batch_size"] = Any(1);
    auto testerPtr = buildTester(KernelTesterBuilder());
    ASSERT_TRUE(testerPtr.get());
    ASSERT_FALSE(testerPtr->hasError());
    TablePtr outputTable;
    bool eof = false;

    {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "uid", {0, 0}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, leftDocs, "cid", {1, 4}));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(testerPtr->setInput("input0", leftTable, true));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(
            _allocator, rightDocs, "cid", {0, 1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(
            _allocator, rightDocs, "group_name", {"groupA", "groupB", "groupC", "groupD"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    ASSERT_EQ("", testerPtr->getErrorMessage());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupB"}));

    {
        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 1);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, rightDocs, "cid", {4}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, rightDocs, "group_name", {"groupE"}));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(testerPtr->setInput("input1", rightTable, true));
    }
    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_FALSE(eof);
    ASSERT_EQ(0, outputTable->getRowCount());

    ASSERT_TRUE(testerPtr->compute());
    ASSERT_EQ(EC_NONE, testerPtr->getErrorCode());
    getOutputTable(testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupE"}));
}

TEST_F(HashJoinKernelTest, testMakeHashJoin) {
    HashJoinKernel kernel;
    auto *naviRHelper = getNaviRHelper();
    ASSERT_TRUE(naviRHelper->getOrCreateRes<JoinInfoCollectorR>(kernel._joinInfoR));
    ASSERT_TRUE(naviRHelper->getOrCreateRes<HashJoinMapR>(kernel._hashJoinMapR));
    JoinBaseParamR joinParamR;
    kernel._joinParamR = &joinParamR;
    {
        MatchDocAllocatorPtr lallocator;
        int64_t leftSize = 100; // original value = 1000000
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(lallocator, leftSize);
        vector<vector<int64_t>> leftIds;
        vector<MultiString> leftGroups;
        for (int64_t i = 0; i < leftSize; ++i) {
            leftIds.push_back({i, i + 1, i + 2});
            vector<string> subs = {
                StringUtil::toString(i), StringUtil::toString(i + 1), StringUtil::toString(i + 2)};
            char *buf = MultiValueCreator::createMultiStringBuffer(subs, _poolPtr.get());
            MultiString multiGroup;
            multiGroup.init(buf);
            leftGroups.push_back(multiGroup);
        }
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int64_t>(
            lallocator, leftDocs, "lid", leftIds));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<MultiString>(
            lallocator, leftDocs, "lgroup", leftGroups));
        kernel._leftBuffer
            = dynamic_pointer_cast<TableData>(createTable(lallocator, leftDocs))->getTable();
    }
    {
        MatchDocAllocatorPtr rallocator;
        int64_t rightSize = 500;
        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(rallocator, rightSize);
        vector<int64_t> rightIds;
        vector<string> rightGroups;
        for (int64_t i = 0; i < rightSize; ++i) {
            rightIds.push_back(i * 1000);
            rightGroups.push_back(autil::StringUtil::toString(i * 1000));
        }
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(rallocator, rightDocs, "rid", rightIds));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(rallocator, rightDocs, "rgroup", rightGroups));
        kernel._rightBuffer
            = (dynamic_pointer_cast<TableData>(createTable(rallocator, rightDocs)))->getTable();
    }
    kernel._joinParamR->_leftJoinColumns = {"lid"};
    kernel._joinParamR->_rightJoinColumns = {"rid"};
    {
        kernel._hashLeftTable = false;
        int64_t st = TimeUtility::currentTime();
        ASSERT_TRUE(kernel.createHashMap(
            kernel._rightBuffer, 0, kernel._rightBuffer->getRowCount(), kernel._hashLeftTable));
        int64_t mid0 = TimeUtility::currentTime();
        HashJoinMapR::HashValues largeTableValues;
        ASSERT_TRUE(kernel._hashJoinMapR->getHashValues(kernel._leftBuffer,
                                                        0,
                                                        kernel._leftBuffer->getRowCount(),
                                                        kernel._joinParamR->_leftJoinColumns,
                                                        largeTableValues));
        cout << "right hash count:" << largeTableValues.size() << endl;
        int64_t mid1 = TimeUtility::currentTime();
        auto count = kernel.makeHashJoin(largeTableValues);
        int64_t end = TimeUtility::currentTime();
        cout << "[hash right id] create:" << mid0 - st << ", hash:" << mid1 - mid0
             << ", join:" << end - mid1 << ", count:" << count << endl;
    }
    kernel._joinParamR->_leftJoinColumns = {"lgroup"};
    kernel._joinParamR->_rightJoinColumns = {"rgroup"};
    {
        kernel._hashLeftTable = false;
        int64_t st = TimeUtility::currentTime();
        ASSERT_TRUE(kernel.createHashMap(
            kernel._rightBuffer, 0, kernel._rightBuffer->getRowCount(), kernel._hashLeftTable));
        int64_t mid0 = TimeUtility::currentTime();
        HashJoinMapR::HashValues largeTableValues;
        ASSERT_TRUE(kernel._hashJoinMapR->getHashValues(kernel._leftBuffer,
                                                        0,
                                                        kernel._leftBuffer->getRowCount(),
                                                        kernel._joinParamR->_leftJoinColumns,
                                                        largeTableValues));
        cout << "right hash count:" << largeTableValues.size() << endl;
        int64_t mid1 = TimeUtility::currentTime();
        auto count = kernel.makeHashJoin(largeTableValues);
        int64_t end = TimeUtility::currentTime();
        cout << "[hash right group] create:" << mid0 - st << ", hash:" << mid1 - mid0
             << ", join:" << end - mid1 << ", count:" << count << endl;
    }

    {
        MatchDocAllocatorPtr lallocator;
        int64_t leftSize = 300; // original value = 3000000
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(lallocator, leftSize);
        vector<int64_t> leftIds;
        vector<string> leftGroups;
        for (int64_t i = 0; i < leftSize; ++i) {
            leftIds.push_back(i);
            leftGroups.push_back(StringUtil::toString(i + 2));
        }
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int64_t>(lallocator, leftDocs, "lid", leftIds));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(lallocator, leftDocs, "lgroup", leftGroups));
        kernel._leftBuffer
            = dynamic_pointer_cast<TableData>(createTable(lallocator, leftDocs))->getTable();
    }
    kernel._joinParamR->_leftJoinColumns = {"lid"};
    kernel._joinParamR->_rightJoinColumns = {"rid"};
    {
        kernel._hashLeftTable = false;
        int64_t st = TimeUtility::currentTime();
        ASSERT_TRUE(kernel.createHashMap(
            kernel._rightBuffer, 0, kernel._rightBuffer->getRowCount(), kernel._hashLeftTable));
        int64_t mid0 = TimeUtility::currentTime();
        HashJoinMapR::HashValues largeTableValues;
        ASSERT_TRUE(kernel._hashJoinMapR->getHashValues(kernel._leftBuffer,
                                                        0,
                                                        kernel._leftBuffer->getRowCount(),
                                                        kernel._joinParamR->_leftJoinColumns,
                                                        largeTableValues));
        cout << "right hash count:" << largeTableValues.size() << endl;
        int64_t mid1 = TimeUtility::currentTime();
        auto count = kernel.makeHashJoin(largeTableValues);
        int64_t end = TimeUtility::currentTime();
        cout << "[hash right id] create:" << mid0 - st << ", hash:" << mid1 - mid0
             << ", join:" << end - mid1 << ", count:" << count << endl;
    }
    kernel._joinParamR->_leftJoinColumns = {"lgroup"};
    kernel._joinParamR->_rightJoinColumns = {"rgroup"};
    {
        kernel._hashLeftTable = false;
        int64_t st = TimeUtility::currentTime();
        ASSERT_TRUE(kernel.createHashMap(
            kernel._rightBuffer, 0, kernel._rightBuffer->getRowCount(), kernel._hashLeftTable));
        int64_t mid0 = TimeUtility::currentTime();
        HashJoinMapR::HashValues largeTableValues;
        ASSERT_TRUE(kernel._hashJoinMapR->getHashValues(kernel._leftBuffer,
                                                        0,
                                                        kernel._leftBuffer->getRowCount(),
                                                        kernel._joinParamR->_leftJoinColumns,
                                                        largeTableValues));
        cout << "right hash count:" << largeTableValues.size() << endl;
        int64_t mid1 = TimeUtility::currentTime();
        auto count = kernel.makeHashJoin(largeTableValues);
        int64_t end = TimeUtility::currentTime();
        cout << "[hash right group] create:" << mid0 - st << ", hash:" << mid1 - mid0
             << ", join:" << end - mid1 << ", count:" << count << endl;
    }
}

TEST_F(HashJoinKernelTest, testLeftJoinLeftFullTable) {
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$uid", 1]})json");

    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 3, 2}, {1, 2, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable2({1, 2}, {"groupB", "groupC"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({3}, {2}, {2}, {"groupC"}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable2({3, 4}, {"groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({2, 0}, {3, 1}, {3, 0}, {"groupD", ""}, true));
}

TEST_F(HashJoinKernelTest, testLeftJoinWithDefaultValue) {
    _attributeMap["hints"]
        = ParseJson(R"json({"JOIN_ATTR":{"defaultValue":"INTEGER:10,VARCHAR:aa"}})json");
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$uid", 1]})json");

    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 3, 2}, {1, 2, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable2({1, 2}, {"groupB", "groupC"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({3}, {2}, {2}, {"groupC"}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable2({3, 4}, {"groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({2, 0}, {3, 1}, {3, 10}, {"groupD", "aa"}, true));
}

TEST_F(HashJoinKernelTest, testLeftJoinRightFullTable) {
    _attributeMap["left_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
    _attributeMap["right_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$cid0", "$group_name0", "$uid", "$cid"])json");
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$uid", 1]})json");

    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 3, 2}, {1, 2, 3}, true, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, false, true));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable2({1, 2}, {"groupB", "groupC"}, false, true));
    ASSERT_NO_FATAL_FAILURE(
        checkOutput({3, 0, 0}, {2, 0, 0}, {2, 0, 1}, {"groupC", "groupA", "groupB"}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable2({3, 4}, {"groupD", "groupE"}, true, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({2, 0}, {3, 0}, {3, 4}, {"groupD", "groupE"}, true));
}

TEST_F(HashJoinKernelTest, testLeftJoinLeftFullTableBatchOutput) {
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$uid", 1]})json");
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 3, 2}, {1, 2, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable2({1, 2}, {"groupB", "groupC"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({}, {}, {}, {}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable2({3, 4}, {"groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({3}, {2}, {2}, {"groupC"}, false));

    ASSERT_NO_FATAL_FAILURE(checkOutput({2}, {3}, {3}, {"groupD"}, false));

    ASSERT_NO_FATAL_FAILURE(checkOutput({0}, {1}, {0}, {""}, true));
}

TEST_F(HashJoinKernelTest, testLeftJoinRightFullTableBatchOutput) {
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["left_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
    _attributeMap["right_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
    _attributeMap["output_fields"]
        = ParseJson(R"json(["$cid0", "$group_name0", "$uid", "$cid"])json");
    _attributeMap["join_type"] = string("LEFT");
    _attributeMap["remaining_condition"]
        = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$uid", 1]})json");

    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 3, 2}, {1, 2, 3}, true, false));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, false, true));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareTable2({1, 2}, {"groupB", "groupC"}, false, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({0, 0}, {0, 0}, {0, 1}, {"groupA", "groupB"}, false));

    ASSERT_NO_FATAL_FAILURE(prepareTable2({3, 4}, {"groupD", "groupE"}, true, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({3}, {2}, {2}, {"groupC"}, false));

    ASSERT_NO_FATAL_FAILURE(checkOutput({2}, {3}, {3}, {"groupD"}, false));

    ASSERT_NO_FATAL_FAILURE(checkOutput({0}, {0}, {4}, {"groupE"}, true));
}

TEST_F(HashJoinKernelTest, testLeftJoinMultiFieldWithEmpty) {
    _attributeMap["join_type"] = string("LEFT");
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid0\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable3({0, 1}, {{0, 1}, {}}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, true));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(!eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupA"}));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {""}));
}

TEST_F(HashJoinKernelTest, testLeftJoinMultiFieldWithAllEmpty) {
    _attributeMap["join_type"] = string("LEFT");
    string anyUdfStr = R"json({"op":"ANY", "params":["$cid"], "type":"UDF"})json";
    string condition = "{\"op\":\"=\", \"params\":[" + anyUdfStr + ", \"$cid0\"]}";
    _attributeMap["equi_condition"] = _attributeMap["condition"] = ParseJson(condition);
    ASSERT_NO_FATAL_FAILURE(semiBuildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareTable3({0, 1}, {{}, {}}, true));
    ASSERT_NO_FATAL_FAILURE(prepareTable2({0}, {"groupA"}, true));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());

    TablePtr outputTable;
    bool eof = false;
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {0, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"", ""}));
}

TEST_F(HashJoinKernelTest, testTruncate) {
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["hints"] = ParseJson(R"json({"JOIN_ATTR":{"truncateThreshold":"1"}})json");
    buildTester(KernelTesterBuilder());
    ASSERT_TRUE(_testerPtr.get());
    ASSERT_FALSE(_testerPtr->hasError());
    ASSERT_NO_FATAL_FAILURE(prepareTable1({0, 0}, {1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(
        prepareTable2({0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));

    ASSERT_TRUE(_testerPtr->compute());
    ASSERT_EQ(EC_NONE, _testerPtr->getErrorCode());
    TablePtr outputTable;
    bool eof = false;
    getOutputTable(_testerPtr, outputTable, eof);
    ASSERT_TRUE(eof);
    ASSERT_EQ(4, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "uid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(outputTable, "cid0", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", {"groupB"}));
}

} // namespace sql
