#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/Column.h>
#include <ha3/sql/ops/test/OpTestBase.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class ColumnTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    void createTable() {
        const auto &docs = getMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        _table.reset(new Table(docs, _allocator));
    }
public:
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, ColumnTest);

void ColumnTest::setUp() {
}

void ColumnTest::tearDown() {
}

TEST_F(ColumnTest, testSimple) {
    createTable();
    ColumnPtr column = _table->getColumn("id");
    ASSERT_EQ(4, column->getRowCount());
    const auto columnData = column->getColumnData<uint32_t>();
    ASSERT_TRUE(NULL != columnData);
    ASSERT_TRUE(NULL != column->getColumnSchema());
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
    ColumnPtr column = _table->getColumn("id");
    ASSERT_EQ(4, column->getRowCount());
    const auto columnData = column->getColumnData<int32_t>();
    ASSERT_TRUE(NULL == columnData);
}

END_HA3_NAMESPACE(sql);
