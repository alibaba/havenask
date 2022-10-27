#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/ColumnData.h>
#include <ha3/sql/rank/ColumnComparator.h>
#include <ha3/sql/rank/ComparatorCreator.h>
#include <ha3/sql/ops/test/OpTestBase.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class ComparatorCreatorTest : public OpTestBase {
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

public:
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, ComparatorCreatorTest);

void ComparatorCreatorTest::setUp() {
}

void ComparatorCreatorTest::tearDown() {
}

TEST_F(ComparatorCreatorTest, testSimple) {
    createTable();
    auto comparator = ComparatorCreator::createComboComparator(_table, {"a", "b"}, {false, false}, &_pool);
    ASSERT_TRUE(comparator);
    const auto &rows = _table->getRows();
    ASSERT_TRUE(comparator->compare(rows[0], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[0]));
    ASSERT_TRUE(comparator->compare(rows[1], rows[2]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[1]));
    ASSERT_TRUE(comparator->compare(rows[0], rows[2]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[2]));
}

TEST_F(ComparatorCreatorTest, testMultiDimension) {
    createTable();
    auto comparator = ComparatorCreator::createComboComparator(_table, {"c", "d"}, {false, false}, &_pool);
    ASSERT_TRUE(comparator);
    const auto &rows = _table->getRows();
    ASSERT_TRUE(comparator->compare(rows[0], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[2]));
    ASSERT_TRUE(comparator->compare(rows[2], rows[1]));
    ASSERT_TRUE(comparator->compare(rows[0], rows[2]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[2]));
}

TEST_F(ComparatorCreatorTest, testInvalidColumnName) {
    createTable();
    auto comparator = ComparatorCreator::createComboComparator(_table, {"c", "dd"}, {false, false}, &_pool);
    ASSERT_FALSE(comparator);
}

TEST_F(ComparatorCreatorTest, testSameColumnName) {
    createTable();
    auto comparator = ComparatorCreator::createComboComparator(_table, {"c", "d", "d", "c", "c", "d"}, {false, false, true, true, true, false}, &_pool);
    ASSERT_TRUE(comparator);
    const auto &rows = _table->getRows();
    ASSERT_TRUE(comparator->compare(rows[0], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[2]));
    ASSERT_TRUE(comparator->compare(rows[2], rows[1]));
    ASSERT_TRUE(comparator->compare(rows[0], rows[2]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[2]));
}

TEST_F(ComparatorCreatorTest, testNoComparator) {
    createTable();
    auto comparator = ComparatorCreator::createComboComparator(_table, {}, {}, &_pool);
    ASSERT_TRUE(comparator);
    const auto &rows = _table->getRows();
    ASSERT_FALSE(comparator->compare(rows[0], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[2]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[0], rows[2]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[0], rows[0]));
    ASSERT_FALSE(comparator->compare(rows[1], rows[1]));
    ASSERT_FALSE(comparator->compare(rows[2], rows[2]));
}

END_HA3_NAMESPACE(sql);
