#include <ha3/sql/ops/test/OpTestBase.h>
#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include "ha3/sql/ops/join/LeftJoin.h"
#include "ha3/sql/data/TableUtil.h"
#include <ha3/sql/ops/condition/ConditionParser.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class LeftJoinTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
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

        _joinBase.reset(new LeftJoin({{"uid", "group_name"}, {"cid", "group_name"}, {"out_uid", "out_group", "out_cid", "out_group0"}, &_joinInfo, _poolPtr, nullptr, _calcTable}));
    }
private:
    MatchDocAllocatorPtr _allocator;
    TablePtr _leftTable;
    TablePtr _rightTable;
    CalcTablePtr _calcTable;
    JoinInfo _joinInfo;
    LeftJoinPtr _joinBase;
    HA3_LOG_DECLARE();
};

HA3_LOG_SETUP(sql, LeftJoinTest);

void LeftJoinTest::setUp() {
}

void LeftJoinTest::tearDown() {
}

TEST_F(LeftJoinTest, testGenerateResultTableLeftFullTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_EQ(2, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10});
}

TEST_F(LeftJoinTest, testGenerateResultTableRightFullTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));
    ASSERT_FALSE(outputTable->isDeletedRow(2));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1, 2});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10, 0});
}

TEST_F(LeftJoinTest, testMultiGenerateResultTableLeftFullTable)
{
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

TEST_F(LeftJoinTest, testMultiGenerateResultTableRightFullTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
    ASSERT_EQ(3, outputTable->getRowCount());
    ASSERT_TRUE(outputTable->isDeletedRow(0));
    ASSERT_FALSE(outputTable->isDeletedRow(1));
    ASSERT_FALSE(outputTable->isDeletedRow(2));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1, 2});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10, 0});


    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {0, 1}, _leftTable, _rightTable, 2, outputTable));
    ASSERT_TRUE(_joinBase->finish(_leftTable, _leftTable->getRowCount(), outputTable));
    ASSERT_EQ(6, outputTable->getRowCount()) << TableUtil::toString(outputTable);
    ASSERT_FALSE(outputTable->isDeletedRow(3));
    ASSERT_TRUE(outputTable->isDeletedRow(4));
    ASSERT_FALSE(outputTable->isDeletedRow(5));

    checkOutputColumn<uint32_t>(outputTable, "out_uid", {2, 1, 2, 2, 1, 1});
    checkOutputColumn<uint32_t>(outputTable, "out_cid", {11, 10, 0, 10, 11, 0});
}

TEST_F(LeftJoinTest, testFinishLeftFullTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
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

TEST_F(LeftJoinTest, testFinishRightFullTable)
{
    ASSERT_NO_FATAL_FAILURE(prepareJoinBase());
    TablePtr outputTable = {};
    ASSERT_TRUE(_joinBase->generateResultTable({0, 1}, {1, 0}, _leftTable, _rightTable, 2, outputTable));
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

TEST_F(LeftJoinTest, testFillNotJoinedRows)
{
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

END_HA3_NAMESPACE();
