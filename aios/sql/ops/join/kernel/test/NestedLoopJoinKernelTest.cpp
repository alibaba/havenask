#include "sql/ops/join/NestedLoopJoinKernel.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
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
#include "navi/engine/Data.h"
#include "navi/engine/Kernel.h"
#include "navi/tester/KernelTester.h"
#include "navi/tester/KernelTesterBuilder.h"
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

class NestedLoopJoinKernelTest : public OpTestBase {
public:
    NestedLoopJoinKernelTest();
    ~NestedLoopJoinKernelTest();

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
        _attributeMap["remaining_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$cid", "$cid0"]})json");
    }

    void prepareSemiAttribute() {
        _attributeMap["join_type"] = string("SEMI");
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields_internal"]
            = ParseJson(R"json(["$uid", "$cid", "$cid0", "$group_name0"])json");
        _attributeMap["output_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["remaining_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$cid", "$cid0"]})json");
    }

    void prepareAntiAttribute() {
        _attributeMap["join_type"] = string("ANTI");
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields_internal"]
            = ParseJson(R"json(["$uid", "$cid", "$cid0", "$group_name0"])json");
        _attributeMap["output_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["remaining_condition"] = _attributeMap["condition"]
            = ParseJson(R"json({"op":">", "type":"OTHER", "params":["$cid", "$cid0"]})json");
    }

    void prepareAttributeNoCondition() {
        _attributeMap.clear();
        _attributeMap["join_type"] = string("INNER");
        _attributeMap["is_equi_join"] = true;
        _attributeMap["left_input_fields"] = ParseJson(R"json(["$uid", "$cid"])json");
        _attributeMap["right_input_fields"] = ParseJson(R"json(["$cid", "$group_name"])json");
        _attributeMap["system_field_num"] = Any(0);
        _attributeMap["output_fields"]
            = ParseJson(R"json(["$uid", "$cid", "$cid0", "$group_name0"])json");
        _attributeMap["condition"] = true;
    }
    void getOutputTable(KernelTesterPtr testerPtr, TablePtr &outputTable, bool &eof) {
        DataPtr odata;
        ASSERT_TRUE(testerPtr->getOutput("output0", odata, eof));
        ASSERT_TRUE(odata != nullptr);
        outputTable = getTable(odata);
        ASSERT_TRUE(outputTable != NULL);
    }

    void buildTester(KernelTesterBuilder builder) {
        setResource(builder);
        builder.kernel("sql.NestedLoopJoinKernel");
        builder.input("input0");
        builder.input("input1");
        builder.output("output0");
        builder.attrs(autil::legacy::FastToJsonString(_attributeMap));
        _testerPtr = builder.build();
        ASSERT_TRUE(_testerPtr.get());
        ASSERT_FALSE(_testerPtr->hasError());
    }

    void prepareLeftTable(const vector<uint32_t> &uid, const vector<uint32_t> &cid, bool eof) {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, uid.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "uid", uid));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "cid", cid));
        auto leftTable = createTable(_allocator, leftDocs);
        ASSERT_TRUE(_testerPtr->setInput("input0", leftTable, eof));
    }

    void prepareRightTable(const vector<uint32_t> &cid, const vector<string> &group, bool eof) {
        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, cid.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, rightDocs, "cid", cid));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, rightDocs, "group_name", group));
        auto rightTable = createTable(_allocator, rightDocs);
        ASSERT_TRUE(_testerPtr->setInput("input1", rightTable, eof));
    }

    void prepareLeftTable(const vector<uint32_t> &uid,
                          const vector<uint32_t> &cid,
                          bool eof,
                          DataPtr &table) {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, uid.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "uid", uid));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "cid", cid));
        auto leftTable = createTable(_allocator, leftDocs);
        table = leftTable;
        ASSERT_TRUE(_testerPtr->setInput("input0", leftTable, eof));
    }

    void prepareRightTable(const vector<uint32_t> &cid,
                           const vector<string> &group,
                           bool eof,
                           DataPtr &table) {
        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, cid.size());
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, rightDocs, "cid", cid));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator(_allocator, rightDocs, "group_name", group));
        auto rightTable = createTable(_allocator, rightDocs);
        table = rightTable;
        ASSERT_TRUE(_testerPtr->setInput("input1", rightTable, eof));
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

    void checkOutput(const vector<uint32_t> &uid,
                     const vector<uint32_t> &cid,
                     const vector<uint32_t> &cid0,
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
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "uid", uid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "cid", cid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "cid0", cid0));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(outputTable, "group_name0", group));
    }

    void checkSemiOutput(const vector<uint32_t> &uid,
                         const vector<uint32_t> &cid,
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
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "uid", uid));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "cid", cid));
    }

public:
    MatchDocAllocatorPtr _allocator;

private:
    KernelTesterPtr _testerPtr;
};

NestedLoopJoinKernelTest::NestedLoopJoinKernelTest() {}

NestedLoopJoinKernelTest::~NestedLoopJoinKernelTest() {}

TEST_F(NestedLoopJoinKernelTest, testWaitFullTable) {
    _attributeMap["buffer_limit_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    auto kernel = dynamic_cast<NestedLoopJoinKernel *>(_testerPtr->getKernel());
    ASSERT_TRUE(kernel->waitFullTable());
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, true));
    auto context = _testerPtr->getComputeContext();
    navi::PortIndex portIndex0(0, navi::INVALID_INDEX);
    navi::PortIndex portIndex1(1, navi::INVALID_INDEX);
    ASSERT_TRUE(
        kernel->getInput(*context, portIndex0, true, 1, kernel->_leftBuffer, kernel->_leftEof));
    ASSERT_TRUE(kernel->_leftFullTable);
    ASSERT_FALSE(kernel->_fullTableCreated);
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_TRUE(
        kernel->getInput(*context, portIndex1, false, 1, kernel->_rightBuffer, kernel->_rightEof));
    ASSERT_FALSE(kernel->waitFullTable());
    kernel->_bufferLimitSize = 10;

    ASSERT_TRUE(kernel->waitFullTable());
    ASSERT_TRUE(kernel->_leftFullTable);
    ASSERT_TRUE(kernel->_fullTableCreated);

    kernel->_leftEof = false;
    ASSERT_TRUE(kernel->waitFullTable());
    ASSERT_FALSE(kernel->_leftFullTable);
    ASSERT_TRUE(kernel->_fullTableCreated);
}

TEST_F(NestedLoopJoinKernelTest, testJoinTable) {
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    auto kernel = dynamic_cast<NestedLoopJoinKernel *>(_testerPtr->getKernel());
    vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
    table::TablePtr leftTable(new table::Table(leftDocs, _allocator));
    vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 3);
    table::TablePtr rightTable(new table::Table(rightDocs, _allocator));
    ASSERT_EQ(0, kernel->_tableAIndexes.size());
    ASSERT_EQ(0, kernel->_tableBIndexes.size());
    size_t joinedRowCount = kernel->joinTable(leftTable, rightTable);
    ASSERT_EQ(2, joinedRowCount);
    vector<size_t> expectA = {0, 0, 0, 1, 1, 1};
    vector<size_t> expectB = {0, 1, 2, 0, 1, 2};
    ASSERT_EQ(expectA, kernel->_tableAIndexes);
    ASSERT_EQ(expectB, kernel->_tableBIndexes);
}

TEST_F(NestedLoopJoinKernelTest, testSimpleProcess) {
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11, 11, 11},
                                        {1, 3, 3, 3},
                                        {0, 0, 1, 2},
                                        {"groupA", "groupA", "groupB", "groupC"},
                                        true));
}

TEST_F(NestedLoopJoinKernelTest, testReuseInput) {
    // reuse is empty
    {
        DataPtr leftTable, rightTable;
        ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
        ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, true, leftTable));
        ASSERT_NO_FATAL_FAILURE(prepareRightTable(
            {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true, rightTable));
        ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11, 11, 11},
                                            {1, 3, 3, 3},
                                            {0, 0, 1, 2},
                                            {"groupA", "groupA", "groupB", "groupC"},
                                            true));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(getTable(leftTable), "uid", {10, 11}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(getTable(leftTable), "cid", {1, 3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(getTable(rightTable), "cid", {}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(getTable(rightTable), "group_name", {}));
    }
    // reuse input
    {
        DataPtr leftTable, rightTable;
        _attributeMap["reuse_inputs"] = ParseJson(R"json([0])json");
        ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
        ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, true, leftTable));
        ASSERT_NO_FATAL_FAILURE(prepareRightTable(
            {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true, rightTable));
        ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11, 11, 11},
                                            {1, 3, 3, 3},
                                            {0, 0, 1, 2},
                                            {"groupA", "groupA", "groupB", "groupC"},
                                            true));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(getTable(leftTable), "uid", {10, 11}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(getTable(leftTable), "cid", {1, 3}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn<uint32_t>(getTable(rightTable), "cid", {0, 1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(
            checkOutputColumn(getTable(rightTable),
                              "group_name",
                              {"groupA", "groupB", "groupC", "groupD", "groupE"}));
    }
}

TEST_F(NestedLoopJoinKernelTest, testEof) {
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());
    ASSERT_TRUE(_testerPtr->setInputEof("input0"));
    ASSERT_TRUE(_testerPtr->setInput("input1", nullptr, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11, 11, 11},
                                        {1, 3, 3, 3},
                                        {0, 0, 1, 2},
                                        {"groupA", "groupA", "groupB", "groupC"},
                                        false));
    ASSERT_TRUE(_testerPtr->setInputEof("input1"));
    ASSERT_NO_FATAL_FAILURE(checkOutputEof());
}

TEST_F(NestedLoopJoinKernelTest, testBatchInputLeftFullTable) {
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10}, {1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({0, 1}, {"groupA", "groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({11}, {3}, true));
    ASSERT_TRUE(_testerPtr->setInput("input1", nullptr, false));
    ASSERT_NO_FATAL_FAILURE(
        checkOutput({10, 11, 11}, {1, 3, 3}, {0, 0, 1}, {"groupA", "groupA", "groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({2, 3}, {"groupC", "groupD"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({11}, {3}, {2}, {"groupC"}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({4}, {"groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({}, {}, {}, {}, true));
}

TEST_F(NestedLoopJoinKernelTest, testBatchInputRightFullTable) {
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10}, {1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({0, 1}, {"groupA", "groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({11}, {3}, false));
    ASSERT_NO_FATAL_FAILURE(checkNoOutput());

    ASSERT_NO_FATAL_FAILURE(prepareRightTable({2, 3}, {"groupC", "groupD"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11, 11, 11},
                                        {1, 3, 3, 3},
                                        {0, 0, 1, 2},
                                        {"groupA", "groupA", "groupB", "groupC"},
                                        false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12}, {5}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({12, 12, 12, 12},
                                        {5, 5, 5, 5},
                                        {0, 1, 2, 3},
                                        {"groupA", "groupB", "groupC", "groupD"},
                                        true));
}

TEST_F(NestedLoopJoinKernelTest, testBatchOutput) {
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11}, {1, 3}, {0, 0}, {"groupA", "groupA"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({11}, {3}, {1}, {"groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({11}, {3}, {2}, {"groupC"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({}, {}, {}, {}, true));
}

TEST_F(NestedLoopJoinKernelTest, testLeftJoinLeftFullTable) {
    _attributeMap["join_type"] = string("LEFT");
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10, 11}, {0, 1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({0, 1}, {"groupA", "groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(
        checkOutput({10, 11, 11}, {1, 3, 3}, {0, 0, 1}, {"groupA", "groupA", "groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({2, 3, 4}, {"groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({11, 12}, {3, 0}, {2, 0}, {"groupC", ""}, true));
}

TEST_F(NestedLoopJoinKernelTest, testLeftJoinRightFullTable) {
    _attributeMap["join_type"] = string("LEFT");
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10}, {0, 1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10, 12}, {1, 0}, {0, 0}, {"groupA", ""}, false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({11}, {3}, true));
    ASSERT_NO_FATAL_FAILURE(
        checkOutput({11, 11, 11}, {3, 3, 3}, {0, 1, 2}, {"groupA", "groupB", "groupC"}, true));
}

TEST_F(NestedLoopJoinKernelTest, testLeftJoinLeftFullTableBatchOutput) {
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["join_type"] = string("LEFT");
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10, 11}, {0, 1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11}, {1, 3}, {0, 0}, {"groupA", "groupA"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({11}, {3}, {1}, {"groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({11}, {3}, {2}, {"groupC"}, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({12}, {0}, {0}, {""}, true));
}

TEST_F(NestedLoopJoinKernelTest, testLeftJoinRightFullTableBatchOutput) {
    _attributeMap["batch_size"] = Any(1);
    _attributeMap["join_type"] = string("LEFT");
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10}, {0, 1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({12}, {0}, {0}, {""}, false));
    ASSERT_TRUE(_testerPtr->setInput("input0", nullptr, false));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10}, {1}, {0}, {"groupA"}, false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({11}, {3}, true));
    ASSERT_NO_FATAL_FAILURE(
        checkOutput({11, 11, 11}, {3, 3, 3}, {0, 1, 2}, {"groupA", "groupB", "groupC"}, true));
}

TEST_F(NestedLoopJoinKernelTest, testSemiJoinLeftFullTable) {
    prepareSemiAttribute();
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10, 11, 13}, {0, 1, 3, 1}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({0, 1}, {"groupA", "groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10, 11, 13}, {1, 3, 1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({2, 3, 4}, {"groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, true));
}

TEST_F(NestedLoopJoinKernelTest, testSemiJoinRightFullTable) {
    prepareSemiAttribute();
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10}, {0, 1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10}, {1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10, 11}, {1, 3}, true));
}

TEST_F(NestedLoopJoinKernelTest, testSemiJoinLeftFullTableBatchOutput) {
    prepareSemiAttribute();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10, 11}, {0, 1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10, 11}, {1, 3}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({2, 3, 4}, {"groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, true));
}

TEST_F(NestedLoopJoinKernelTest, testSemiJoinRightFullTableBatchOutput) {
    prepareSemiAttribute();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10}, {0, 1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10}, {1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({11}, {3}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({11}, {3}, false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({11}, {3}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({11}, {3}, true));
}

TEST_F(NestedLoopJoinKernelTest, testAntiJoinLeftFullTable) {
    prepareAntiAttribute();
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10, 11, 13}, {0, 1, 3, 1}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({0, 1}, {"groupA", "groupB"}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({2, 3, 4}, {"groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({12}, {0}, true));
}

TEST_F(NestedLoopJoinKernelTest, testAntiJoinRightFullTable) {
    prepareAntiAttribute();
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10}, {0, 1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({12}, {0}, false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {0, 0}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({10, 11}, {0, 0}, true));
}

TEST_F(NestedLoopJoinKernelTest, testAntiJoinLeftFullTableBatchOutput) {
    prepareAntiAttribute();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10, 11}, {0, 1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({2, 3}, {"groupC", "groupD"}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({12}, {0}, true));
}

TEST_F(NestedLoopJoinKernelTest, testAntiJoinRightFullTableBatchOutput) {
    prepareAntiAttribute();
    _attributeMap["batch_size"] = Any(1);
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({12, 10}, {0, 1}, false));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable(
        {0, 1, 2, 3, 4}, {"groupA", "groupB", "groupC", "groupD", "groupE"}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({12}, {0}, false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({11}, {3}, false));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({}, {}, false));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({11}, {0}, true));
    ASSERT_NO_FATAL_FAILURE(checkSemiOutput({11}, {0}, true));
}

TEST_F(NestedLoopJoinKernelTest, testNoCondition) {
    prepareAttributeNoCondition();
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({0, 1}, {"groupA", "groupB"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11, 10, 11},
                                        {1, 3, 1, 3},
                                        {0, 0, 1, 1},
                                        {"groupA", "groupA", "groupB", "groupB"},
                                        true));
}

TEST_F(NestedLoopJoinKernelTest, testNoCondition2) {
    prepareAttributeNoCondition();
    _attributeMap["remaining_condition"] = _attributeMap["condition"];
    ASSERT_NO_FATAL_FAILURE(buildTester(KernelTesterBuilder()));
    ASSERT_NO_FATAL_FAILURE(prepareLeftTable({10, 11}, {1, 3}, true));
    ASSERT_NO_FATAL_FAILURE(prepareRightTable({0, 1}, {"groupA", "groupB"}, true));
    ASSERT_NO_FATAL_FAILURE(checkOutput({10, 11, 10, 11},
                                        {1, 3, 1, 3},
                                        {0, 0, 1, 1},
                                        {"groupA", "groupA", "groupB", "groupB"},
                                        true));
}

} // namespace sql
