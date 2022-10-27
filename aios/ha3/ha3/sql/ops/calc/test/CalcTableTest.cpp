#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/ops/calc/CalcKernel.h>
#include <navi/engine/KernelTesterBuilder.h>
#include <ha3/sql/ops/condition/ConditionParser.h>
#include <ha3/sql/ops/util/KernelUtil.h>

using namespace std;
using namespace matchdoc;
using namespace suez::turing;
using namespace navi;
using namespace autil::legacy;
using namespace autil::legacy::json;

BEGIN_HA3_NAMESPACE(sql);

class CalcTableTest : public OpTestBase {
public:
    CalcTableTest();
    ~CalcTableTest();
public:
    void setUp() override {
    }
    void tearDown() override {
        _table.reset();
        _calcTable.reset();
    }
private:
    void prepareTable() {
        _allocator.reset(new matchdoc::MatchDocAllocator(_memPoolResource.getPool()));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "a", {5, 6, 7, 8}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "b", {"b1", "b2", "b3", "b4"}));
        ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<char>(_allocator, docs, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
        _table.reset(new Table(docs, _allocator));
    }

    void prepareCalcTable(const std::vector<std::string> &outputFields = {},
                          const std::vector<std::string> &outputFieldsType = {}) {
        _calcTable.reset(new CalcTable(&_pool, &_memPoolResource,
                        outputFields, outputFieldsType, NULL, NULL,
                        suez::turing::CavaPluginManagerPtr(), NULL));
    }
public:
    MatchDocAllocatorPtr _allocator;
    TablePtr _table;
    CalcTablePtr _calcTable;
    tensorflow::QueryResourcePtr _queryResource;
    tensorflow::SessionResourcePtr _sessionResource;
    SqlQueryResourcePtr _sqlQueryResource;
    SqlBizResourcePtr _sqlBizResource;
};

CalcTableTest::CalcTableTest() {
}

CalcTableTest::~CalcTableTest() {
}


TEST_F(CalcTableTest, testFilterTable) {
    {
        prepareCalcTable();
        _allocator.reset(new matchdoc::MatchDocAllocator(_memPoolResource.getPool()));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "AVG(t2.price)", {1, 2, 3, 4}));
        _table.reset(new Table(docs, _allocator));

        string conditionStr = "{\"op\":\">\", \"params\":[\"$AVG(t2.price)\", 1]}";
        ConditionParser parser(_memPoolResource.getPool().get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));
        _calcTable->setFilterFlag(true);
        ASSERT_TRUE(_calcTable->filterTable(_table));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "AVG(t2.price)", {2, 3, 4}));
    }
    {
        prepareCalcTable();
        _allocator.reset(new matchdoc::MatchDocAllocator(_memPoolResource.getPool()));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "AVG(t2.price)", {1, 2, 3, 4}));
        _table.reset(new Table(docs, _allocator));

        string conditionStr = "{\"op\":\">\", \"params\":[\"$AVG(t2.price)\", 1]}";
        ConditionParser parser(_memPoolResource.getPool().get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));
        _calcTable->setFilterFlag(false);
        ASSERT_TRUE(_calcTable->filterTable(_table));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "AVG(t2.price)", {1, 2, 3, 4}));
    }
    {
        prepareCalcTable();
        _allocator.reset(new matchdoc::MatchDocAllocator(_memPoolResource.getPool()));
        vector<MatchDoc> docs = _allocator->batchAllocate(4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "AVG(t2.price)", {1, 2, 3, 4}));
        _table.reset(new Table(docs, _allocator));

        string conditionStr = "";
        ConditionParser parser(_memPoolResource.getPool().get());
        ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));
        _calcTable->setFilterFlag(true);
        ASSERT_TRUE(_calcTable->filterTable(_table));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "AVG(t2.price)", {1, 2, 3, 4}));
    }
}

TEST_F(CalcTableTest, testFilterTableWith$) {
    prepareCalcTable();
    _allocator.reset(new matchdoc::MatchDocAllocator(_memPoolResource.getPool()));
    vector<MatchDoc> docs = _allocator->batchAllocate(4);
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "$f1", {1, 2, 3, 4}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "a", {5, 6, 7, 8}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "b", {"b1", "b2", "b3", "b4"}));
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<char>(_allocator, docs, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
    _table.reset(new Table(docs, _allocator));

    string conditionStr = "{\"op\":\">\", \"params\":[\"$$f1\", 1]}";
    ConditionParser parser(_memPoolResource.getPool().get());
    ASSERT_TRUE(parser.parseCondition(conditionStr, _calcTable->_condition));
    ASSERT_TRUE(_calcTable->filterTable(_table));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "$f1", {2, 3, 4}));
}


TEST_F(CalcTableTest, testCloneColumn) {
    prepareTable();
    prepareCalcTable();
    auto poolPtr = _memPoolResource.getPool();
    {
        TablePtr output(new Table(poolPtr));
        output->batchAllocateRow(4);
        ASSERT_TRUE(_calcTable->cloneColumn(_table->getColumn("id"), output, "a", false));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(output, "a", {1, 2, 3, 4}));
        ASSERT_TRUE(_calcTable->cloneColumn(_table->getColumn("c"), output, "c", false));
        ASSERT_NO_FATAL_FAILURE(checkOutputMultiColumn<char>(output, "c", {{'c', '1'}, {'2'}, {'3'}, {'4'}}));
    }
    {
        TablePtr output(new Table(poolPtr));
        output->batchAllocateRow(4);
        ASSERT_TRUE(_calcTable->cloneColumn(_table->getColumn("id"), output, "a", false));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(output, "a", {1, 2, 3, 4}));
        ASSERT_FALSE(_calcTable->cloneColumn(_table->getColumn("id"), output, "a", true));
    }
    {
        TablePtr output(new Table(poolPtr));
        output->batchAllocateRow(4);
        ASSERT_TRUE(_calcTable->cloneColumn(_table->getColumn("id"), output, "a", false));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(output, "a", {1, 2, 3, 4}));
        ASSERT_TRUE(_calcTable->cloneColumn(_table->getColumn("id"), output, "b", false));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(output, "b", {1, 2, 3, 4}));
    }
}

TEST_F(CalcTableTest, testAddAlaisMap) {
    prepareTable();
    map<string, string> alaisMap = {{"_a_", "a"}, {"_id_", "id"}};
    ASSERT_TRUE(_allocator->getReferenceAliasMap() == NULL);
    CalcTable::addAliasMap(_allocator.get(), alaisMap);
    ASSERT_TRUE(_allocator->getReferenceAliasMap() != NULL);
    ASSERT_EQ(2, _allocator->getReferenceAliasMap()->size());

    map<string, string> alaisMap2 = {{"_a1_", "a"}, {"_id_", "id"}};
    CalcTable::addAliasMap(_allocator.get(), alaisMap2);
    ASSERT_TRUE(_allocator->getReferenceAliasMap() != NULL);
    ASSERT_EQ(3, _allocator->getReferenceAliasMap()->size());
}

TEST_F(CalcTableTest, testNeedCopyTable) {
    prepareCalcTable();
    TablePtr table(new Table(_memPoolResource.getPool()));
    {
        _calcTable->_exprsMap = {{"a", ExprEntity()}};
        ASSERT_TRUE(_calcTable->needCopyTable(table));
    }
    _calcTable->_exprsMap = {};
    table->declareColumn<int>("a");
    table->declareColumn<int>("b");
    {
        _calcTable->_outputFields = {"a"};
        ASSERT_TRUE(_calcTable->needCopyTable(table));
    }
    {
        _calcTable->_outputFields = {"b", "a"};
        ASSERT_TRUE(_calcTable->needCopyTable(table));
    }
    {
        _calcTable->_outputFields = {"a", "b"};
        ASSERT_FALSE(_calcTable->needCopyTable(table));
    }
}

END_HA3_NAMESPACE(sql);
