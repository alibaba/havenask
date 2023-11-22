#include "table/ColumnComparator.h"

#include <cstdint>
#include <string>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "table/Column.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;

namespace table {

class ColumnComparatorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void createTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(_allocator, docs, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(_allocator, docs, "b", {"1", "2", "3"}));
        _table = Table::fromMatchDocs(docs, _allocator);
        ASSERT_TRUE(_table);
    }

public:
    autil::mem_pool::Pool _pool;
    MatchDocUtil _matchDocUtil;
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
};

void ColumnComparatorTest::setUp() {}

void ColumnComparatorTest::tearDown() {}

TEST_F(ColumnComparatorTest, testSimple) {
    createTable();
    auto column = _table->getColumn("id");
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
    auto column = _table->getColumn("id");
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
    auto column = _table->getColumn("a");
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
    auto column = _table->getColumn("b");
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

} // namespace table
