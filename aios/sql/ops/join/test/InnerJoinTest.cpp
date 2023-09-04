#include "sql/ops/join/InnerJoin.h"

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

class InnerJoinTest : public OpTestBase {
public:
    void setUp();
    void tearDown();

public:
    void prepareJoinBase() {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "uid", {2, 1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "group_name", {100, 101}));
        _leftTable.reset(new Table(leftDocs, _allocator));

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, rightDocs, "cid", {10, 11}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, rightDocs, "group_name", {99, 101}));
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
        _joinBase.reset(new InnerJoin(_param));
    }

private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _leftTable;
    TablePtr _rightTable;
    CalcTableRPtr _calcTableR;
    JoinInfo _joinInfo;
    JoinBaseParam _param;
    InnerJoinPtr _joinBase;
};

void InnerJoinTest::setUp() {}

void InnerJoinTest::tearDown() {}

TEST_F(InnerJoinTest, testGenerateResultTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10});
}

TEST_F(InnerJoinTest, testMultiGenerateResultTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10});

    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {0, 1}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(4, outputTable->getRowCount());
    ASSERT_FALSE(outputTable->isDeletedRow(2));
    ASSERT_TRUE(outputTable->isDeletedRow(3));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1, 2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10, 10, 11});
}

} // namespace sql
