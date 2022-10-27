#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/ColumnData.h>
#include <ha3/sql/rank/ColumnComparator.h>
#include <ha3/sql/rank/ComboComparator.h>
#include <ha3/sql/ops/test/OpTestBase.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class ComboComparatorTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    void createTable() {
        const auto &docs = getMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<float>(_allocator, docs, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "b", {"1", "2", "3"}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "c", {1, 2, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "d", {1, 1, 2}));
        _table.reset(new Table(docs, _allocator));
    }

    template <class T>
    void createComparator(const string &name, bool flag, Comparator *&ret) {
        ColumnPtr column = _table->getColumn(name);
        ASSERT_EQ(3, column->getRowCount());
        ASSERT_TRUE(nullptr != column);
        const auto &columnData = column->getColumnData<T>();
        if (flag) {
            ret = POOL_NEW_CLASS((&_pool), ColumnDescComparator<T>, columnData);
        } else {
            ret = POOL_NEW_CLASS((&_pool), ColumnAscComparator<T>, columnData);
        }
    }
public:
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, ComboComparatorTest);

void ComboComparatorTest::setUp() {
}

void ComboComparatorTest::tearDown() {
}

TEST_F(ComboComparatorTest, testSimple) {
    createTable();
    Comparator* tmp1 = NULL;
    Comparator* tmp2 = NULL;
    createComparator<uint32_t>("id", false, tmp1);
    createComparator<float>("a", true, tmp2);
    ComboComparator comparator;
    comparator.addComparator(tmp1);
    comparator.addComparator(tmp2);
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


TEST_F(ComboComparatorTest, testMultiDimension) {
    createTable();
    Comparator* tmp1;
    Comparator* tmp2;
    createComparator<uint32_t>("c", false, tmp1);
    createComparator<uint32_t>("d", false, tmp2);
    ComboComparator comparator;
    comparator.addComparator(tmp1);
    comparator.addComparator(tmp2);
    const auto &rows = _table->getRows();
    ASSERT_TRUE(comparator.compare(rows[0], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[2]));
    ASSERT_TRUE(comparator.compare(rows[2], rows[1]));
    ASSERT_TRUE(comparator.compare(rows[0], rows[2]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator.compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator.compare(rows[2], rows[2]));
}

END_HA3_NAMESPACE(sql);
