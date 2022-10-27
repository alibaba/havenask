#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/join/AntiJoin.h"
#include "ha3/sql/data/TableUtil.h"
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class AntiJoinTest : public OpTestBase {
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
        string conditionStr =
            R"json({"op" : ">", "params" : ["$group_name", "$group_name0"]})json";
        ConditionParser parser(_memPoolResource.getPool().get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));

        _joinBase.reset(new AntiJoin({{"uid", "group_name"},
                            {"cid", "group_name"},
                            {"uid", "group_name", "cid", "group_name0"},
                                &_joinInfo, _poolPtr, nullptr, _calcTable}));
    }

private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _leftTable;
    TablePtr _rightTable;
    CalcTablePtr _calcTable;
    JoinInfo _joinInfo;
    AntiJoinPtr _joinBase;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, AntiJoinTest);

void AntiJoinTest::setUp() {
}

void AntiJoinTest::tearDown() {
}

TEST_F(AntiJoinTest, testGenerateResultTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0},
                    _leftTable, _rightTable, _leftTable->getRowCount(), outputTable));
    ASSERT_NO_FATAL_FAILURE(checkVector({false, true, false, false}, _joinBase->_joinedFlag));
}

TEST_F(AntiJoinTest, testMultiGenerateResultTableLeftFullTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0},
                    _leftTable, _rightTable, _leftTable->getRowCount(), outputTable));
    ASSERT_NO_FATAL_FAILURE(checkVector({false, true, false, false}, _joinBase->_joinedFlag));
    ASSERT_TRUE(_joinBase->generateResultTable({1, 2, 2, 3, 3}, {0, 2, 3, 2, 3},
                    _leftTable, _rightTable, _leftTable->getRowCount(), outputTable));
    ASSERT_NO_FATAL_FAILURE(checkVector({false, true, true, true}, _joinBase->_joinedFlag));
}

TEST_F(AntiJoinTest, testGenerateResultTableFailed)
{
    navi::NaviLoggerProvider provider;
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_FALSE(_joinBase->generateResultTable({0, 1}, {1, 0},
                    _leftTable, _leftTable, _leftTable->getRowCount(), outputTable));
    CHECK_TRACE_COUNT(1, "init join table failed", provider.getTrace(""));
}

TEST_F(AntiJoinTest, testMultiGenerateResultTableFailed)
{
    navi::NaviLoggerProvider provider;
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0},
                    _leftTable, _rightTable, _leftTable->getRowCount(), outputTable));
    ASSERT_FALSE(_joinBase->generateResultTable({1, 2, 2, 3, 3}, {0, 2, 3, 2, 3},
                    _leftTable, _leftTable, _leftTable->getRowCount(), outputTable));
    CHECK_TRACE_COUNT(1, "evaluate join table failed", provider.getTrace(""));
}

TEST_F(AntiJoinTest, testFinish)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());

    vector<MatchDoc> docs = std::move(getMatchDocs(_allocator, 0));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "uid", {}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "group_name", {}));
    TablePtr outputTable(new Table(docs, _allocator));

    _joinBase->_joinedFlag = {false, true, false, false};
    ASSERT_TRUE(_joinBase->finish(_leftTable, 4, outputTable));
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_EQ(2, outputTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "uid", {2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(outputTable, "group_name", {100, 203, 204}));
    ASSERT_EQ(0, _joinBase->_joinedFlag.size());
}

END_HA3_NAMESPACE();
