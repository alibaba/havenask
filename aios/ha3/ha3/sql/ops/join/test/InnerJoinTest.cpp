#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/join/InnerJoin.h"
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class InnerJoinTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    void prepareJoinBase() {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "uid", {2, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "group_name", {100, 101}));
        _leftTable.reset(new Table(leftDocs, _allocator));

        vector<MatchDoc> rightDocs = std::move(getMatchDocs(_allocator, 2));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, rightDocs, "cid", {10, 11}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, rightDocs, "group_name", {99, 101}));
        _rightTable.reset(new Table(rightDocs, _allocator));


        _calcTable.reset(new CalcTable(&_pool, &_memPoolResource, {}, {}, NULL, NULL, {}, NULL));
        string conditionStr = R"json({"op" : ">", "params" : ["$out_group", "$out_group0"]})json";
        ConditionParser parser(_memPoolResource.getPool().get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));

        _joinBase.reset(new InnerJoin({{"uid", "group_name"}, {"cid", "group_name"}, {"out_uid", "out_group", "out_cid", "out_group0"}, &_joinInfo, _poolPtr, nullptr, _calcTable}));
    }
private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _leftTable;
    TablePtr _rightTable;
    CalcTablePtr _calcTable;
    JoinInfo _joinInfo;
    InnerJoinPtr _joinBase;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, InnerJoinTest);

void InnerJoinTest::setUp() {
}

void InnerJoinTest::tearDown() {
}

TEST_F(InnerJoinTest, testGenerateResultTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10});
}

TEST_F(InnerJoinTest, testMultiGenerateResultTable) {
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10});


    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {0, 1}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(4, outputTable->getRowCount());
    ASSERT_FALSE(outputTable->isDeletedRow(2));
    ASSERT_TRUE(outputTable->isDeletedRow(3));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1, 2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10, 10, 11});
}

END_HA3_NAMESPACE();
