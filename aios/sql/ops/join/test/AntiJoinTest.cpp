#include "sql/ops/join/AntiJoin.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/log/NaviLoggerProvider.h"
#include "navi/tester/NaviResourceHelper.h"
#include "sql/ops/calc/CalcTableR.h"
#include "sql/ops/condition/ConditionParser.h"
#include "sql/ops/join/JoinBase.h"
#include "sql/ops/test/OpTestBase.h"
#include "sql/proto/SqlSearchInfo.pb.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace table;
using namespace testing;
using namespace matchdoc;

namespace sql {

class AntiJoinTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    void prepareJoinBase() {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "uid", {2, 1, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "group_name", {100, 101, 203, 204}));
        _leftTable.reset(new Table(leftDocs, _allocator));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, rightDocs, "cid", {10, 11, 23, 24}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, rightDocs, "group_name", {99, 101, 200, 201}));
        _rightTable.reset(new Table(rightDocs, _allocator));
        _naviRHelper.kernelConfig(R"json(
        {
            "output_fields" : ["a", "id", "b"],
            "output_fields_type" : ["INTEGER", "INTEGER", "BIGINT"]
        }
        )json");
        _calcTableR = _naviRHelper.getOrCreateResPtr<CalcTableR>();
        ASSERT_TRUE(_calcTableR);
        string conditionStr = R"json({"op" : ">", "params" : ["$group_name", "$group_name0"]})json";
        ConditionParser parser(_poolPtr.get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTableR->_condition));

        _param = {{"uid", "group_name"},
                  {"cid", "group_name"},
                  {"uid", "group_name", "cid", "group_name0"},
                  &_joinInfo,
                  _poolPtr,
                  nullptr};
        _param._calcTableR = _calcTableR;
        _joinBase.reset(new AntiJoin(_param));
    }

private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _leftTable;
    TablePtr _rightTable;
    CalcTableRPtr _calcTableR;
    JoinInfo _joinInfo;
    JoinBaseParam _param;
    AntiJoinPtr _joinBase;
};

void AntiJoinTest::setUp() {}

void AntiJoinTest::tearDown() {}

TEST_F(AntiJoinTest, testGenerateResultTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable(
        {0, 1}, {1, 0}, _leftTable, _rightTable, _leftTable->getRowCount(), outputTable));
    ASSERT_NO_FATAL_FAILURE(checkVector({false, true, false, false}, _joinBase->_joinedFlag));
}

TEST_F(AntiJoinTest, testMultiGenerateResultTableLeftFullTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable(
        {0, 1}, {1, 0}, _leftTable, _rightTable, _leftTable->getRowCount(), outputTable));
    ASSERT_NO_FATAL_FAILURE(checkVector({false, true, false, false}, _joinBase->_joinedFlag));
    ASSERT_TRUE(_joinBase->generateResultTable({1, 2, 2, 3, 3},
                                               {0, 2, 3, 2, 3},
                                               _leftTable,
                                               _rightTable,
                                               _leftTable->getRowCount(),
                                               outputTable));
    ASSERT_NO_FATAL_FAILURE(checkVector({false, true, true, true}, _joinBase->_joinedFlag));
}

TEST_F(AntiJoinTest, testGenerateResultTableFailed) {
    navi::NaviLoggerProvider provider;
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_FALSE(_joinBase->generateResultTable(
        {0, 1}, {1, 0}, _leftTable, _leftTable, _leftTable->getRowCount(), outputTable));
    CHECK_TRACE_COUNT(1, "init join table failed", provider.getTrace(""));
}

TEST_F(AntiJoinTest, testMultiGenerateResultTableFailed) {
    navi::NaviLoggerProvider provider;
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable(
        {0, 1}, {1, 0}, _leftTable, _rightTable, _leftTable->getRowCount(), outputTable));
    ASSERT_FALSE(_joinBase->generateResultTable({1, 2, 2, 3, 3},
                                                {0, 2, 3, 2, 3},
                                                _leftTable,
                                                _leftTable,
                                                _leftTable->getRowCount(),
                                                outputTable));
    CHECK_TRACE_COUNT(1, "evaluate join table failed", provider.getTrace(""));
}

TEST_F(AntiJoinTest, testFinish) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());

    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 0);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "uid", {}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "group_name", {}));
    TablePtr outputTable(new Table(docs, _allocator));

    _joinBase->_joinedFlag = {false, true, false, false};
    ASSERT_TRUE(_joinBase->finish(_leftTable, 4, outputTable));
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "uid", {2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(
        checkOutputColumn<uint32_t>(outputTable, "group_name", {100, 203, 204}));
    ASSERT_EQ(0, _joinBase->_joinedFlag.size());
}

} // namespace sql
