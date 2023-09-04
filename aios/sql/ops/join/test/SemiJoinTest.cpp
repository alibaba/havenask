#include "sql/ops/join/SemiJoin.h"

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
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

class SemiJoinTest : public OpTestBase {
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
        string conditionStr = R"json({"op" : ">", "params" : ["$out_group", "$out_group0"]})json";
        ConditionParser parser(_poolPtr.get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTableR->_condition));

        _param = {{"uid", "group_name"},
                  {"cid", "group_name"},
                  {"out_uid", "out_group", "out_cid", "out_group0"},
                  &_joinInfo,
                  _poolPtr,
                  nullptr};
        _param._calcTableR = _calcTableR;
        _joinBase.reset(new SemiJoin(_param));
    }

private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _leftTable;
    TablePtr _rightTable;
    CalcTableRPtr _calcTableR;
    JoinInfo _joinInfo;
    JoinBaseParam _param;
    SemiJoinPtr _joinBase;
};

void SemiJoinTest::setUp() {}

void SemiJoinTest::tearDown() {}

TEST_F(SemiJoinTest, testGenerateResultTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {101});
}

TEST_F(SemiJoinTest, testMultiGenerateResultTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {101});

    ASSERT_TRUE(_joinBase->finish(_leftTable, 2, outputTable));

    outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable(
        {1, 2, 2, 3, 3}, {0, 2, 3, 2, 3}, _leftTable, _rightTable, 4, outputTable));
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1, 3, 4});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {101, 203, 204});
}

TEST_F(SemiJoinTest, testMultiGenerateResultTableLeftFullTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {101});

    outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable(
        {1, 2, 2, 3, 3}, {0, 2, 3, 2, 3}, _leftTable, _rightTable, 4, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {3, 4});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {203, 204});

    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
}

} // namespace sql
