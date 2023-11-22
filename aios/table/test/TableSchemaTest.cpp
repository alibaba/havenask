#include "table/TableSchema.h"

#include "gtest/gtest.h"
#include <memory>
#include <string>

#include "table/ColumnSchema.h"
#include "table/Table.h"
#include "table/test/MatchDocUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace testing;
using namespace matchdoc;

namespace table {

class TableSchemaTest : public TESTBASE {
protected:
    void createTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(_allocator, docs, "b", {0.1, 1.1, 2.1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int16_t>(_allocator, {}, "empty", {}));
        _table = Table::fromMatchDocs(docs, _allocator);
        ASSERT_TRUE(_table);
    }

public:
    MatchDocUtil _matchDocUtil;
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
};

TEST_F(TableSchemaTest, testSimple) {
    createTable();
    auto a = _table->getTableSchema().getColumn("a");
    ASSERT_EQ("a", a->getName());
    ASSERT_EQ(matchdoc::bt_int32, a->getType().getBuiltinType());
}

TEST_F(TableSchemaTest, testFunc) {
    ColumnSchemaPtr schema(new ColumnSchema("a", ValueTypeHelper<int32_t>::getValueType()));
    TableSchema tableSchema;
    ASSERT_FALSE(tableSchema.getColumn("a"));
    ASSERT_TRUE(tableSchema.addColumn(schema));
    ASSERT_TRUE(tableSchema.getColumn("a"));
    ASSERT_FALSE(tableSchema.addColumn(schema));
}

} // namespace table
