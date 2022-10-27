#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/ColumnData.h>
#include <ha3/sql/rank/ColumnComparator.h>
#include <ha3/sql/ops/test/OpTestBase.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class ColumnComparatorTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    void createTable() {
        const auto &docs = getMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(_allocator, docs, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "b", {"1", "2", "3"}));
        _table.reset(new Table(docs, _allocator));
    }
public:
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, ColumnComparatorTest);

void ColumnComparatorTest::setUp() {
}

void ColumnComparatorTest::tearDown() {
}

TEST_F(ColumnComparatorTest, testSimple) {
    createTable();
    ColumnPtr column = _table->getColumn("id");
    ASSERT_EQ(3, column->getRowCount());
    ASSERT_TRUE(nullptr != column);
    const auto &columnData = column->getColumnData<uint32_t>();
    ColumnAscComparator<uint32_t> comparator(columnData);
    const auto &rows = _table->getRows();
    ASSERT_TRUE(comparator.compare(rows[0], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[0]));
    ASSERT_TRUE(comparator.compare(rows[1], rows[2]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[1]));
    ASSERT_TRUE(comparator.compare(rows[0], rows[2]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[2]));
}

TEST_F(ColumnComparatorTest, testFlag) {
    createTable();
    ColumnPtr column = _table->getColumn("id");
    ASSERT_EQ(3, column->getRowCount());
    ASSERT_TRUE(nullptr != column);
    const auto &columnData = column->getColumnData<uint32_t>();
    ColumnDescComparator<uint32_t> comparator(columnData);
    const auto &rows = _table->getRows();
    ASSERT_FALSE(comparator.compare(rows[0], rows[1]));
    ASSERT_TRUE(comparator.compare(rows[1], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[2]));
    ASSERT_TRUE(comparator.compare(rows[2], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[0], rows[2]));
    ASSERT_TRUE(comparator.compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[2]));
}

TEST_F(ColumnComparatorTest, testFloat) {
    createTable();
    ColumnPtr column = _table->getColumn("a");
    ASSERT_EQ(3, column->getRowCount());
    ASSERT_TRUE(nullptr != column);
    const auto &columnData = column->getColumnData<float>();
    ColumnAscComparator<float> comparator(columnData);
    const auto &rows = _table->getRows();
    ASSERT_TRUE(comparator.compare(rows[0], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[0]));
    ASSERT_TRUE(comparator.compare(rows[1], rows[2]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[1]));
    ASSERT_TRUE(comparator.compare(rows[0], rows[2]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[2]));
}

TEST_F(ColumnComparatorTest, testMultiChar) {
    createTable();
    ColumnPtr column = _table->getColumn("b");
    ASSERT_EQ(3, column->getRowCount());
    ASSERT_TRUE(nullptr != column);
    const auto &columnData = column->getColumnData<autil::MultiChar>();
    ColumnAscComparator<autil::MultiChar> comparator(columnData);
    const auto &rows = _table->getRows();
    ASSERT_TRUE(comparator.compare(rows[0], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[0]));
    ASSERT_TRUE(comparator.compare(rows[1], rows[2]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[1]));
    ASSERT_TRUE(comparator.compare(rows[0], rows[2]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[2]));
}

END_HA3_NAMESPACE(sql);
