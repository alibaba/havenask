#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/Table.h>
#include <ha3/sql/ops/test/OpTestBase.h>

using namespace std;
using namespace autil;
using namespace testing;
using namespace matchdoc;
using namespace autil;

BEGIN_HA3_NAMESPACE(sql);

class TableTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    void createTable() {
        const auto &docs = getMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(_allocator, docs, "b", {0.1, 1.1, 2.1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "c", {"c1", "c2", "c3"}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(_allocator, {}, "empty", {}));
        _table.reset(new Table(docs, _allocator));
    }
public:
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, TableTest);

void TableTest::setUp() {
}

void TableTest::tearDown() {
}

TEST_F(TableTest, testConstruct) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
}

TEST_F(TableTest, testConstructEmpty) {
    const auto &docs = getMatchDocs(_allocator, 0);
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(_allocator, docs, "b", {}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "c", {}));
    _table.reset(new Table(docs, _allocator));
    ASSERT_EQ(0, _table->getRowCount());
    ASSERT_EQ(3, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {}));
}


TEST_F(TableTest, testGetTableRowSize) {
    createTable();
    ASSERT_EQ(18, _table->getRowSize());
}

TEST_F(TableTest, testGetColumn) {
    createTable();
    auto column = _table->getColumn("a");
    ASSERT_TRUE(nullptr != column);
    ASSERT_EQ("a", column->getColumnSchema()->getName());
    ASSERT_EQ(matchdoc::bt_int32, column->getColumnSchema()->getType().getBuiltinType());
    column = _table->getColumn(0);
    ASSERT_TRUE(nullptr != column);
    ASSERT_EQ("a", column->getColumnSchema()->getName());
    ASSERT_EQ(matchdoc::bt_int32, column->getColumnSchema()->getType().getBuiltinType());
    column = _table->getColumn("d");
    ASSERT_TRUE(nullptr == column);
}

TEST_F(TableTest, testDeclareColumn) {
    createTable();
    ASSERT_EQ(4, _table->getColumnCount());
    _table->declareColumn<int16_t>("d");
    ASSERT_EQ(5, _table->getColumnCount());
    auto column = _table->getColumn("d");
    ASSERT_TRUE(nullptr != column);
    ASSERT_TRUE(NULL != column->getColumnData<int16_t>());
    _table->declareColumn<int32_t>("d");
    ASSERT_EQ(5, _table->getColumnCount());
    column = _table->getColumn("d");
    ASSERT_TRUE(nullptr != column);
    ASSERT_TRUE(NULL != column->getColumnData<int16_t>());
    column = _table->declareColumn<std::string>("e");
    ASSERT_TRUE(nullptr == column);
}

TEST_F(TableTest, testColumnName) {
    createTable();
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_EQ("a", _table->getColumnName(0));
    ASSERT_EQ("b", _table->getColumnName(1));
    ASSERT_EQ("c", _table->getColumnName(2));
    ASSERT_EQ("empty", _table->getColumnName(3));
    _table->declareColumn<int32_t>("e");
    ASSERT_EQ("e", _table->getColumnName(4));
    ASSERT_EQ(5, _table->getColumnCount());
}

TEST_F(TableTest, testMerge) {
    createTable();
    MatchDocAllocatorPtr otherAllocator;
    const auto & otherDocs = getMatchDocs(otherAllocator, 2);
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
    TablePtr otherTable(new Table(otherDocs, otherAllocator));
    ASSERT_TRUE(_table->merge(otherTable));
    ASSERT_EQ(5, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3", "c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0, 1, 2}));
}
TEST_F(TableTest, testMergeWithCopy) {
    createTable();
    MatchDocAllocatorPtr otherAllocator;
    const auto & otherDocs = getMatchDocs(otherAllocator, 2);
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
    otherAllocator->declare<int32_t>("aaa");
    TablePtr otherTable(new Table(otherDocs, otherAllocator));

    ASSERT_TRUE(_table->merge(otherTable));
    ASSERT_EQ(5, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3", "c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0, 1, 2}));
}


TEST_F(TableTest, testBadMerge) {
    createTable();
    {
        // test less column
        MatchDocAllocatorPtr otherAllocator;
        const auto & otherDocs = getMatchDocs(otherAllocator, 2);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
        TablePtr otherTable(new Table(otherDocs, otherAllocator));
        ASSERT_FALSE(_table->merge(otherTable));
        ASSERT_EQ(3, _table->getRowCount());
        ASSERT_EQ(4, _table->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
        ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
        // test different value type
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(otherAllocator, {}, "c", {}));
        otherTable.reset(new Table(otherDocs, otherAllocator));
        ASSERT_FALSE(_table->merge(otherTable));
    }
    {
        // test more column
        MatchDocAllocatorPtr otherAllocator;
        const auto & otherDocs = getMatchDocs(otherAllocator, 2);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
        TablePtr otherTable(new Table(otherDocs, otherAllocator));
        ASSERT_FALSE(otherTable->merge(_table));
    }
    {
        // test different column names
        MatchDocAllocatorPtr otherAllocator;
        const auto & otherDocs = getMatchDocs(otherAllocator, 2);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "aa", {4, 5}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
        TablePtr otherTable(new Table(otherDocs, otherAllocator));
        ASSERT_FALSE(_table->merge(otherTable));
    }
}

TEST_F(TableTest, testCopyTable) {
    createTable();
    MatchDocAllocatorPtr otherAllocator;
    const auto & otherDocs = getMatchDocs(otherAllocator, 2);
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
    TablePtr otherTable(new Table(otherDocs, otherAllocator));
    ASSERT_TRUE(_table->copyTable(otherTable));
    ASSERT_EQ(5, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3", "c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0, 1, 2}));
}

TEST_F(TableTest, testCopyTableUseSub) {
    createTable();
    MatchDocAllocatorPtr otherAllocator;
    const auto & otherDocs = getMatchDocsUseSub(otherAllocator, 2);
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
    TablePtr otherTable(new Table(otherDocs, otherAllocator));
    ASSERT_FALSE(_table->copyTable(otherTable));
    ASSERT_FALSE(otherTable->copyTable(_table));
}


TEST_F(TableTest, testAllocateRow) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    _table->allocateRow();
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 0}));
}

TEST_F(TableTest, testBatchAllocateRow) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    _table->batchAllocateRow(2);
    ASSERT_EQ(5, _table->getRowCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 0, 0}));
}

TEST_F(TableTest, testSerializeAndDeserialize) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
    DataBuffer buffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, &_pool);
    _table->serialize(buffer);
    TablePtr other(new Table(_poolPtr));
    other->deserialize(buffer);
    ASSERT_TRUE(_table->getTableSchema() == other->getTableSchema());
    ASSERT_EQ(3, other->getRowCount());
    ASSERT_EQ(4, other->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(other, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(other, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(other, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(other, "empty", {0, 0, 0}));
    ASSERT_TRUE(nullptr != _table->_allocator);
    const auto &schema = _table->getTableSchema();
    ASSERT_TRUE(NULL != schema.getColumnSchema("a"));
    ASSERT_EQ("a", schema.getColumnSchema("a")->getName());
    ASSERT_TRUE(NULL != schema.getColumnSchema("b"));
    ASSERT_EQ("b", schema.getColumnSchema("b")->getName());
    ASSERT_TRUE(NULL != schema.getColumnSchema("empty"));
    ASSERT_EQ("empty", schema.getColumnSchema("empty")->getName());
}

TEST_F(TableTest, testSerializeToString) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
    string data;
    _table->serializeToString(data, &_pool);
    TablePtr other(new Table(_poolPtr));
    ASSERT_FALSE(_table->getTableSchema() == other->getTableSchema());
    other->deserializeFromString(data, &_pool);
    ASSERT_TRUE(_table->getTableSchema() == other->getTableSchema());
    ASSERT_EQ(3, other->getRowCount());
    ASSERT_EQ(4, other->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(other, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(other, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(other, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(other, "empty", {0, 0, 0}));
}

TEST_F(TableTest, testGetColumnName) {
    createTable();
    ASSERT_EQ("a", _table->getColumnName(0));
    ASSERT_EQ("b", _table->getColumnName(1));
    ASSERT_EQ("c", _table->getColumnName(2));
    ASSERT_EQ("empty", _table->getColumnName(3));
}

TEST_F(TableTest, testSetRows) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
    auto rows = _table->getRows();
    reverse(rows.begin(), rows.end());
    _table->setRows(rows);
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {3, 2, 1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {2.1, 1.1, 0.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c3", "c2", "c1"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
}

TEST_F(TableTest, testDeleteRows) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
    _table->markDeleteRow(1);
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    _table->deleteRows();
    _table->deleteRows();
    ASSERT_EQ(2, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int32_t>(_table, "a", {1, 3}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<float>(_table, "b", {0.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "c", {"c1", "c3"}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int16_t>(_table, "empty", {0, 0}));
}

END_HA3_NAMESPACE(sql);
