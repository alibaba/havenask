#include "table/Column.h"

#include "autil/Log.h"
#include "table/ColumnData.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;

namespace table {

class ColumnTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void createTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        _table = Table::fromMatchDocs(docs, _allocator);
    }

public:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    MatchDocUtil _matchDocUtil;
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, ColumnTest);

void ColumnTest::setUp() {}

void ColumnTest::tearDown() {}

TEST_F(ColumnTest, testSimple) {
    createTable();
    auto column = _table->getColumn("id");
    ASSERT_EQ(4, column->getRowCount());
    const auto columnData = column->getColumnData<uint32_t>();
    ASSERT_TRUE(nullptr != columnData);
    ASSERT_TRUE(nullptr != column->getColumnSchema());
    ASSERT_EQ(1, columnData->get(0));
    ASSERT_EQ(2, columnData->get(1));
    ASSERT_EQ(3, columnData->get(2));
    ASSERT_EQ(4, columnData->get(3));
    for (size_t i = 0; i < column->getRowCount(); i++) {
        ASSERT_EQ(autil::StringUtil::toString(i + 1), column->toString(i));
    }
    _table->allocateRow();
    ASSERT_EQ(5, column->getRowCount());
}

TEST_F(ColumnTest, testWrongType) {
    createTable();
    auto column = _table->getColumn("id");
    ASSERT_EQ(4, column->getRowCount());
    const auto columnData = column->getColumnData<int32_t>();
    ASSERT_TRUE(nullptr == columnData);
}

} // namespace table
