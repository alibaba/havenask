#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/join/SemiJoin.h"
#include "ha3/sql/data/TableUtil.h"
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class SemiJoinTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
    void prepareJoinBase() {
        vector<MatchDoc> leftDocs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "uid", {2, 1, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, leftDocs, "group_name", {100, 101, 203, 204}));
        _leftTable.reset(new Table(leftDocs, _allocator));

        vector<MatchDoc> rightDocs = std::move(getMatchDocs(_allocator, 4));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, rightDocs, "cid", {10, 11, 23, 24}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, rightDocs, "group_name", {99, 101, 200, 201}));
        _rightTable.reset(new Table(rightDocs, _allocator));


        _calcTable.reset(new CalcTable(&_pool, &_memPoolResource, {}, {}, NULL, NULL, {}, NULL));
        string conditionStr = R"json({"op" : ">", "params" : ["$out_group", "$out_group0"]})json";
        ConditionParser parser(_memPoolResource.getPool().get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));

        _joinBase.reset(new SemiJoin({{"uid", "group_name"}, {"cid", "group_name"}, {"out_uid", "out_group", "out_cid", "out_group0"}, &_joinInfo, _poolPtr, nullptr, _calcTable}));
    }
private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _leftTable;
    TablePtr _rightTable;
    CalcTablePtr _calcTable;
    JoinInfo _joinInfo;
    SemiJoinPtr _joinBase;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, SemiJoinTest);

void SemiJoinTest::setUp() {
}

void SemiJoinTest::tearDown() {
}

TEST_F(SemiJoinTest, testGenerateResultTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0},
                    _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {101});
}

TEST_F(SemiJoinTest, testMultiGenerateResultTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0},
                    _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {101});

    ASSERT_TRUE(_joinBase->finish(_leftTable, 2, outputTable));

    outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({1, 2, 2, 3, 3}, {0, 2, 3, 2, 3},
                    _leftTable, _rightTable, 4, outputTable));
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1, 3, 4});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {101, 203, 204});
}

TEST_F(SemiJoinTest, testMultiGenerateResultTableLeftFullTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0},
                    _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(1, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {1});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {101});

    outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({1, 2, 2, 3, 3}, {0, 2, 3, 2, 3},
                    _leftTable, _rightTable, 4, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    checkOutputColumn<uint32_t>(outputTable, "out_uid", {3, 4});
    checkOutputColumn<uint32_t>(outputTable, "out_group", {203, 204});

    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
}

END_HA3_NAMESPACE();
