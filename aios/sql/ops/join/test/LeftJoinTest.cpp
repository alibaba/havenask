#include "sql/ops/join/LeftJoin.h"

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
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace table;
using namespace testing;
using namespace matchdoc;

namespace sql {

class LeftJoinTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    void prepareJoinBase() {
        vector<MatchDoc> leftDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "uid", {2, 1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, leftDocs, "group_name", {100, 101}));
        _leftTable = Table::fromMatchDocs(leftDocs, _allocator);

        vector<MatchDoc> rightDocs = _matchDocUtil.createMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, rightDocs, "cid", {10, 11}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(
            _allocator, rightDocs, "group_name", {99, 101}));
        _rightTable = Table::fromMatchDocs(rightDocs, _allocator);
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

        auto *naviRHelper = getNaviRHelper();
        ASSERT_TRUE(naviRHelper->getOrCreateRes(_param._joinInfoR));
        _param._leftInputFields = {"uid", "group_name"};
        _param._rightInputFields = {"cid", "group_name"};
        _param._outputFields = {"out_uid", "out_group", "out_cid", "out_group0"};
        _param._poolPtr = _poolPtr;
        _param._pool = nullptr;
        _param._calcTableR = _calcTableR;
        _joinBase.reset(new LeftJoin(_param));
    }

private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _leftTable;
    TablePtr _rightTable;
    CalcTableRPtr _calcTableR;
    JoinBaseParamR _param;
    LeftJoinPtr _joinBase;
};

void LeftJoinTest::setUp() {}

void LeftJoinTest::tearDown() {}

TEST_F(LeftJoinTest, testGenerateResultTableLeftFullTable) {
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

TEST_F(LeftJoinTest, testGenerateResultTableRightFullTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));
    ASSERT_FALSE(outputTable->isDeletedRow(2));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1, 2});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10, 0});
}

TEST_F(LeftJoinTest, testMultiGenerateResultTableLeftFullTable) {
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

TEST_F(LeftJoinTest, testMultiGenerateResultTableRightFullTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));
    ASSERT_FALSE(outputTable->isDeletedRow(2));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1, 2});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10, 0});

    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {0, 1}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
    ASSERT_EQ(6, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_FALSE(outputTable->isDeletedRow(3));
    ASSERT_TRUE(outputTable->isDeletedRow(4));
    ASSERT_FALSE(outputTable->isDeletedRow(5));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1, 2, 2, 1, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10, 0, 10, 11, 0});
}

TEST_F(LeftJoinTest, testFinishLeftFullTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10});

    outputTable->deleteRows();
    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1, 2});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {10, 0});
}

TEST_F(LeftJoinTest, testFinishRightFullTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(
        _joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10});

    outputTable->deleteRows();
    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1, 2});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {10, 0});
}

TEST_F(LeftJoinTest, testFillNotJoinedRows) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable(new Table(_poolPtr));
    ASSERT_TRUE(_joinBase->initJoinedTable(_leftTable, _rightTable, outputTable));
    _joinBase->_joinedFlag = {1, 0, 1, 1, 0};

    _joinBase->fillNotJoinedRows(_leftTable, 2, outputTable);

    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "out_uid", {1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "out_group", {101}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "out_cid", {0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "out_group0", {0}));

    ASSERT_EQ(3, _joinBase->_joinedFlag.size());
    ASSERT_EQ(1, _joinBase->_joinedFlag[0]);
    ASSERT_EQ(1, _joinBase->_joinedFlag[1]);
    ASSERT_EQ(0, _joinBase->_joinedFlag[2]);
}

} // namespace sql
