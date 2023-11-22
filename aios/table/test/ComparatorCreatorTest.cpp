#include "table/ComparatorCreator.h"

#include <cstdint>
#include <iosfwd>
#include <memory>
#include <string>
#include <vector>

#include "autil/mem_pool/Pool.h"
#include "table/ComboComparator.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;

namespace table {

class ComparatorCreatorTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void createTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(_allocator, docs, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(_allocator, docs, "b", {"1", "2", "3"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "c", {1, 2, 1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "d", {1, 1, 2}));
        _table = Table::fromMatchDocs(docs, _allocator);
        ASSERT_TRUE(_table);
    }

public:
    autil::mem_pool::Pool _pool;
    MatchDocUtil _matchDocUtil;
    TablePtr _table;
    matchdoc::MatchDocAllocatorPtr _allocator;
};

void ComparatorCreatorTest::setUp() {}

void ComparatorCreatorTest::tearDown() {}

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
    auto comparator = ComparatorCreator::createComboComparator(
        _table, {"c", "d", "d", "c", "c", "d"}, {false, false, true, true, true, false}, &_pool);
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

} // namespace table
