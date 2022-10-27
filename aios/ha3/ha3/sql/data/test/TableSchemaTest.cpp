#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/Column.h>
#include <ha3/sql/ops/test/OpTestBase.h>

using namespace std;
using namespace testing;
using namespace matchdoc;

BEGIN_HA3_NAMESPACE(sql);

class TableSchemaTest : public OpTestBase {
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

AUTIL_LOG_SETUP(table, TableSchemaTest);

void TableSchemaTest::setUp() {
}

void TableSchemaTest::tearDown() {
}

TEST_F(TableSchemaTest, testSimple) {
    createTable();
    auto a = _table->getTableSchema().getColumnSchema("a");
    ASSERT_EQ("a", a->getName());
    ASSERT_EQ(matchdoc::bt_int32, a->getType().getBuiltinType());
}

TEST_F(TableSchemaTest, testFunc) {
    ColumnSchemaPtr schema(new ColumnSchema("a", ValueTypeHelper<int32_t>::getValueType()));
    TableSchema tableSchema;
    ASSERT_FALSE(tableSchema.getColumnSchema("a"));
    ASSERT_TRUE(tableSchema.addColumnSchema(schema));
    ASSERT_TRUE(tableSchema.getColumnSchema("a"));
    ASSERT_FALSE(tableSchema.addColumnSchema(schema));

    ASSERT_TRUE(tableSchema.eraseColumnSchema("a"));
    ASSERT_FALSE(tableSchema.getColumnSchema("a"));
    ASSERT_FALSE(tableSchema.eraseColumnSchema("a"));

    ASSERT_TRUE(tableSchema.addColumnSchema(schema));
    ASSERT_TRUE(tableSchema.getColumnSchema("a"));
}

END_HA3_NAMESPACE(sql);
