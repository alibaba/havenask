#include "table/Table.h"

#include <algorithm>
#include <cstddef>
#include <memory>
#include <stdint.h>
#include <string>
#include <vector>

#include "autil/CommonMacros.h"
#include "autil/CompressionUtil.h"
#include "autil/DataBuffer.h"
#include "table/Column.h"
#include "table/ColumnSchema.h"
#include "table/TableSchema.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

using namespace std;
using namespace autil;
using namespace testing;
using namespace matchdoc;
using namespace autil;

namespace table {

class TableTest : public TESTBASE {
public:
    void createTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 3);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(_allocator, docs, "b", {0.1, 1.1, 2.1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(_allocator, docs, "c", {"c1", "c2", "c3"}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int16_t>(_allocator, {}, "empty", {}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
            _allocator, docs, "multi_string_field", {{}, {"c2", ""}, {"c4", "c5", "c6", "c7"}}));
        _table = Table::fromMatchDocs(docs, _allocator);
        ASSERT_TRUE(_table);
    }

protected:
    void doTestSerializeToString(autil::CompressType type = autil::CompressType::NO_COMPRESS) {
        createTable();
        ASSERT_EQ(3, _table->getRowCount());
        ASSERT_EQ(5, _table->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
        string data;
        std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
        _table->serializeToString(data, pool.get(), type);
        TablePtr other(new Table(pool));
        ASSERT_FALSE(_table->getTableSchema() == other->getTableSchema());
        other->deserializeFromString(data, pool.get());
        ASSERT_TRUE(_table->getTableSchema() == other->getTableSchema());
        ASSERT_EQ(3, other->getRowCount());
        ASSERT_EQ(5, other->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(other, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(other, "b", {0.1, 1.1, 2.1}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(other, "c", {"c1", "c2", "c3"}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(other, "empty", {0, 0, 0}));
    }

public:
    MatchDocUtil _matchDocUtil;
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
};

TEST_F(TableTest, testConstruct) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
}

TEST_F(TableTest, testConstructEmpty) {
    const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 0);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, docs, "a", {}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(_allocator, docs, "b", {}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(_allocator, docs, "c", {}));
    _table = Table::fromMatchDocs(docs, _allocator);
    ASSERT_TRUE(_table);
    ASSERT_EQ(0, _table->getRowCount());
    ASSERT_EQ(3, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {}));
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
    ASSERT_EQ(5, _table->getColumnCount());
    _table->declareColumn<int16_t>("d");
    ASSERT_EQ(6, _table->getColumnCount());
    auto column = _table->getColumn("d");
    ASSERT_TRUE(nullptr != column);
    ASSERT_TRUE(nullptr != column->getColumnData<int16_t>());
    _table->declareColumn<int32_t>("d");
    ASSERT_EQ(6, _table->getColumnCount());
    column = _table->getColumn("d");
    ASSERT_TRUE(nullptr != column);
    ASSERT_TRUE(nullptr != column->getColumnData<int16_t>());
    Column *columnPtr = _table->declareColumn<std::string>("e");
    ASSERT_EQ(nullptr, columnPtr);
}

TEST_F(TableTest, testColumnName) {
    createTable();
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_EQ("a", _table->getColumn(0)->getName());
    ASSERT_EQ("b", _table->getColumn(1)->getName());
    ASSERT_EQ("c", _table->getColumn(2)->getName());
    ASSERT_EQ("empty", _table->getColumn(3)->getName());
    _table->declareColumn<int32_t>("e");
    ASSERT_EQ("e", _table->getColumn(5)->getName());
    ASSERT_EQ(6, _table->getColumnCount());
}

TEST_F(TableTest, testMerge) {
    createTable();
    MatchDocAllocatorPtr otherAllocator;
    const auto &otherDocs = _matchDocUtil.createMatchDocs(otherAllocator, 2);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
        otherAllocator, otherDocs, "multi_string_field", {{}, {"c2", ""}}));
    TablePtr otherTable = Table::fromMatchDocs(otherDocs, otherAllocator);
    ASSERT_TRUE(otherAllocator);
    ASSERT_TRUE(_table->merge(otherTable));
    ASSERT_EQ(5, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3", "c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0, 1, 2}));
}

TEST_F(TableTest, testMergeWithCopy) {
    createTable();
    MatchDocAllocatorPtr otherAllocator;
    const auto &otherDocs = _matchDocUtil.createMatchDocs(otherAllocator, 2);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
        otherAllocator, otherDocs, "multi_string_field", {{}, {"c2", ""}}));
    otherAllocator->declare<int32_t>("aaa");
    TablePtr otherTable = Table::fromMatchDocs(otherDocs, otherAllocator);
    ASSERT_TRUE(otherTable);

    ASSERT_TRUE(_table->merge(otherTable));
    ASSERT_EQ(5, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3", "c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0, 1, 2}));
}

TEST_F(TableTest, testBadMerge) {
    createTable();
    {
        // test less column
        MatchDocAllocatorPtr otherAllocator;
        const auto &otherDocs = _matchDocUtil.createMatchDocs(otherAllocator, 2);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
        TablePtr otherTable = Table::fromMatchDocs(otherDocs, otherAllocator);
        ASSERT_TRUE(otherTable);
        ASSERT_FALSE(_table->merge(otherTable));
        ASSERT_EQ(3, _table->getRowCount());
        ASSERT_EQ(5, _table->getColumnCount());
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
        // test different value type
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int16_t>(otherAllocator, {}, "c", {}));
        otherTable = Table::fromMatchDocs(otherDocs, otherAllocator);
        ASSERT_TRUE(otherTable);
        ASSERT_FALSE(_table->merge(otherTable));
    }
    {
        // test more column
        MatchDocAllocatorPtr otherAllocator;
        const auto &otherDocs = _matchDocUtil.createMatchDocs(otherAllocator, 2);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
        TablePtr otherTable = Table::fromMatchDocs(otherDocs, otherAllocator);
        ASSERT_FALSE(otherTable->merge(_table));
    }
    {
        // test different column names
        MatchDocAllocatorPtr otherAllocator;
        const auto &otherDocs = _matchDocUtil.createMatchDocs(otherAllocator, 2);
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "aa", {4, 5}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
        ASSERT_NO_FATAL_FAILURE(
            _matchDocUtil.extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
        TablePtr otherTable = Table::fromMatchDocs(otherDocs, otherAllocator);
        ASSERT_FALSE(_table->merge(otherTable));
    }
}

TEST_F(TableTest, testProjectTable) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    auto other = _table->project({{"x", "a"}, {"y", "b"}}).unwrap();
    auto a = _table->getColumn("a");
    auto as = a->getColumnData<int32_t>();
    auto z = other->declareColumn("z", matchdoc::BuiltinType::bt_int32, false);
    auto zs = z->getColumnData<int32_t>();
    ASSERT_TRUE(zs != nullptr);
    zs->set(0, 2);
    zs->set(1, 4);
    zs->set(2, 6);
    ASSERT_EQ(3, other->getRowCount());
    ASSERT_EQ(3, other->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(other, "x", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(other, "y", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(other, "z", {2, 4, 6}));
    as->set(1, 5);
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(other, "x", {1, 5, 3}));
}

TEST_F(TableTest, testCopyTable) {
    createTable();
    MatchDocAllocatorPtr otherAllocator;
    const auto &otherDocs = _matchDocUtil.createMatchDocs(otherAllocator, 2);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<string>(
        otherAllocator, otherDocs, "multi_string_field", {{}, {"c2", ""}}));
    TablePtr otherTable = Table::fromMatchDocs(otherDocs, otherAllocator);
    ASSERT_TRUE(_table->copyTable(otherTable.get()));
    ASSERT_EQ(5, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 4, 5}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3", "c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0, 1, 2}));
}

TEST_F(TableTest, testCopyTableUseSub) {
    createTable();
    MatchDocAllocatorPtr otherAllocator;
    const auto &otherDocs = _matchDocUtil.createMatchDocsUseSub(otherAllocator, 2);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int32_t>(otherAllocator, otherDocs, "a", {4, 5}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<float>(otherAllocator, {}, "b", {}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(otherAllocator, otherDocs, "c", {"c4", "c5"}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int16_t>(otherAllocator, otherDocs, "empty", {1, 2}));
    TablePtr otherTable = Table::fromMatchDocs(otherDocs, otherAllocator);
    ASSERT_FALSE(_table->copyTable(otherTable.get()));
    ASSERT_FALSE(otherTable->copyTable(_table.get()));
}

TEST_F(TableTest, testAllocateRow) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    _table->allocateRow();
    ASSERT_EQ(4, _table->getRowCount());
    Column *a = _table->getColumn("a");
    ASSERT_TRUE(a != nullptr);
    a->getColumnData<int32_t>()->set(3, 0);
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 0}));
}

TEST_F(TableTest, testBatchAllocateRow) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    _table->batchAllocateRow(2);
    ASSERT_EQ(5, _table->getRowCount());
    Column *a = _table->getColumn("a");
    ASSERT_TRUE(a != nullptr);
    a->getColumnData<int32_t>()->set(3, 0);
    a->getColumnData<int32_t>()->set(4, 0);
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3, 0, 0}));
}

TEST_F(TableTest, testSerializeAndDeserialize) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
    std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
    DataBuffer buffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, pool.get());
    _table->serialize(buffer);
    TablePtr other(new Table(pool));
    other->deserialize(buffer);
    ASSERT_TRUE(_table->getTableSchema() == other->getTableSchema());
    ASSERT_EQ(3, other->getRowCount());
    ASSERT_EQ(5, other->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(other, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(other, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(other, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(other, "empty", {0, 0, 0}));
    const auto &schema = _table->getTableSchema();
    ASSERT_TRUE(nullptr != schema.getColumn("a"));
    ASSERT_EQ("a", schema.getColumn("a")->getName());
    ASSERT_TRUE(nullptr != schema.getColumn("b"));
    ASSERT_EQ("b", schema.getColumn("b")->getName());
    ASSERT_TRUE(nullptr != schema.getColumn("empty"));
    ASSERT_EQ("empty", schema.getColumn("empty")->getName());
}

TEST_F(TableTest, testDeserializeAndDelete) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn<string>(
        _table, "multi_string_field", {{}, {"c2", ""}, {"c4", "c5", "c6", "c7"}}));

    auto dataBufferPool = new autil::mem_pool::Pool();
    DataBuffer buffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, dataBufferPool);
    _table->serialize(buffer);

    std::shared_ptr<autil::mem_pool::Pool> tablePool(new autil::mem_pool::Pool());
    TablePtr other(new Table(tablePool));
    other->deserialize(buffer);
    delete dataBufferPool;

    ASSERT_TRUE(_table->getTableSchema() == other->getTableSchema());
    ASSERT_EQ(3, other->getRowCount());
    ASSERT_EQ(5, other->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(other, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(other, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(other, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(other, "empty", {0, 0, 0}));
    ASSERT_NO_FATAL_FAILURE(
        TableTestUtil::checkOutputMultiColumn(other, "multi_string_field", {{}, {"c2", ""}, {"c4", "c5", "c6", "c7"}}));
}

TEST_F(TableTest, testSerializeToString) { doTestSerializeToString(); }

TEST_F(TableTest, testSerializeToStringLZ4) { doTestSerializeToString(autil::CompressType::LZ4); }

TEST_F(TableTest, testSerializeToStringSnappy) { doTestSerializeToString(autil::CompressType::SNAPPY); }

TEST_F(TableTest, testSerializeToStringZlibDefault) {
    doTestSerializeToString(autil::CompressType::Z_DEFAULT_COMPRESS);
}

TEST_F(TableTest, testSerializeToStringZlibSpeed) { doTestSerializeToString(autil::CompressType::Z_SPEED_COMPRESS); }

TEST_F(TableTest, testGetColumnName) {
    createTable();
    ASSERT_EQ("a", _table->getColumn(0)->getName());
    ASSERT_EQ("b", _table->getColumn(1)->getName());
    ASSERT_EQ("c", _table->getColumn(2)->getName());
    ASSERT_EQ("empty", _table->getColumn(3)->getName());
}

TEST_F(TableTest, testSetRows) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
    auto rows = _table->getRows();
    reverse(rows.begin(), rows.end());
    _table->setRows(std::move(rows));
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {3, 2, 1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {2.1, 1.1, 0.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c3", "c2", "c1"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
}

TEST_F(TableTest, testDeleteRows) {
    createTable();
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0, 0}));
    _table->markDeleteRow(1);
    ASSERT_EQ(3, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    _table->deleteRows();
    _table->deleteRows();
    ASSERT_EQ(2, _table->getRowCount());
    ASSERT_EQ(5, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(_table, "a", {1, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(_table, "b", {0.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "c", {"c1", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(_table, "empty", {0, 0}));
}

TEST_F(TableTest, testCloneTable) {
    createTable();
    TablePtr otherTable;
    std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
    otherTable = _table->clone(pool);
    ASSERT_TRUE(otherTable);
    ASSERT_EQ(3, otherTable->getRowCount());
    ASSERT_EQ(5, otherTable->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int32_t>(otherTable, "a", {1, 2, 3}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<float>(otherTable, "b", {0.1, 1.1, 2.1}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(otherTable, "c", {"c1", "c2", "c3"}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int16_t>(otherTable, "empty", {0, 0, 0}));
}

TEST_F(TableTest, testHllCtx) {
    std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
    HllCtx *ctx = Hyperloglog::hllCtxCreate(Hyperloglog::HLL_DENSE, pool.get());
    ASSERT_TRUE(ctx != NULL);
    for (size_t i = 0; i <= 500000; i++) {
        ASSERT_TRUE(Hyperloglog::hllCtxAdd(ctx, (unsigned char *)&i, sizeof(int64_t), pool.get()) != -1);
    }

    TablePtr table = std::make_shared<Table>(pool);
    table->batchAllocateRow(1);
    auto column = table->declareColumn("ctx_field", matchdoc::BuiltinType::bt_hllctx, false);
    ASSERT_TRUE(column != nullptr);
    auto columnData = column->getColumnData<HllCtx>();
    ASSERT_TRUE(columnData != nullptr);
    columnData->set(0, *ctx);
    HllCtx ctx0 = columnData->get(0);
    auto count = autil::Hyperloglog::hllCtxCount(ctx);
    ASSERT_EQ(count, autil::Hyperloglog::hllCtxCount(&ctx0));
    autil::Hyperloglog::hllCtxReset(ctx);
    ASSERT_EQ(count, autil::Hyperloglog::hllCtxCount(&ctx0));

    DataBuffer buffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, nullptr);
    table->serialize(buffer);

    std::shared_ptr<autil::mem_pool::Pool> tablePool(new autil::mem_pool::Pool());
    TablePtr other(new Table(tablePool));
    other->deserialize(buffer);
    auto otherColumn = other->declareColumn("ctx_field", matchdoc::BuiltinType::bt_hllctx, false);
    ASSERT_TRUE(otherColumn != nullptr);
    auto otherColumnData = column->getColumnData<HllCtx>();
    ASSERT_TRUE(otherColumnData != nullptr);
    HllCtx ctx1 = otherColumnData->get(0);
    ASSERT_EQ(count, autil::Hyperloglog::hllCtxCount(&ctx1));
}

TEST_F(TableTest, testHllCtx2Row) {
    std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
    HllCtx *ctx = Hyperloglog::hllCtxCreate(Hyperloglog::HLL_DENSE, pool.get());
    ASSERT_TRUE(ctx != NULL);
    for (size_t i = 0; i <= 500000; i++) {
        ASSERT_TRUE(Hyperloglog::hllCtxAdd(ctx, (unsigned char *)&i, sizeof(int64_t), pool.get()) != -1);
    }
    auto count = autil::Hyperloglog::hllCtxCount(ctx);

    HllCtx *ctx1 = Hyperloglog::hllCtxCreate(Hyperloglog::HLL_DENSE, pool.get());
    ASSERT_TRUE(ctx1 != NULL);
    for (size_t i = 0; i <= 100; i++) {
        ASSERT_TRUE(Hyperloglog::hllCtxAdd(ctx1, (unsigned char *)&i, sizeof(int64_t), pool.get()) != -1);
    }
    auto count1 = autil::Hyperloglog::hllCtxCount(ctx1);

    TablePtr table = std::make_shared<Table>(pool);
    table->batchAllocateRow(2);
    auto column = table->declareColumn("ctx_field", matchdoc::BuiltinType::bt_hllctx, false);
    ASSERT_TRUE(column != nullptr);
    auto columnData = column->getColumnData<HllCtx>();
    ASSERT_TRUE(columnData != nullptr);
    columnData->set(0, *ctx);
    columnData->set(1, *ctx1);

    HllCtx val0 = columnData->get(0);
    ASSERT_EQ(count, autil::Hyperloglog::hllCtxCount(&val0));
    HllCtx val1 = columnData->get(1);
    ASSERT_EQ(count1, autil::Hyperloglog::hllCtxCount(&val1));
}

TEST_F(TableTest, testHllCtxMerge) {
    std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
    HllCtx *ctx = Hyperloglog::hllCtxCreate(Hyperloglog::HLL_DENSE, pool.get());
    ASSERT_TRUE(ctx != NULL);
    for (size_t i = 0; i <= 500000; i++) {
        ASSERT_TRUE(Hyperloglog::hllCtxAdd(ctx, (unsigned char *)&i, sizeof(int64_t), pool.get()) != -1);
    }

    TablePtr table = std::make_shared<Table>(pool);
    table->batchAllocateRow(1);
    auto column = table->declareColumn("ctx_field", matchdoc::BuiltinType::bt_hllctx, false);
    ASSERT_TRUE(column != nullptr);
    auto columnData = column->getColumnData<HllCtx>();
    ASSERT_TRUE(columnData != nullptr);
    columnData->set(0, *ctx);

    TablePtr table1 = std::make_shared<Table>(pool);
    table1->batchAllocateRow(1);
    auto column1 = table1->declareColumn("ctx_field", matchdoc::BuiltinType::bt_hllctx, false);
    ASSERT_TRUE(column1 != nullptr);
    auto columnData1 = column1->getColumnData<HllCtx>();
    ASSERT_TRUE(columnData1 != nullptr);
    columnData1->set(0, *ctx);

    ASSERT_TRUE(table->merge(table1));
    ASSERT_EQ(2, table->getRowCount());

    auto column2 = table->getColumn("ctx_field");
    ASSERT_TRUE(column2 != nullptr);
    auto columnData2 = column2->getColumnData<HllCtx>();
    ASSERT_TRUE(columnData2 != nullptr);

    HllCtx ctx0 = columnData2->get(0);
    auto count = autil::Hyperloglog::hllCtxCount(ctx);
    ASSERT_EQ(count, autil::Hyperloglog::hllCtxCount(&ctx0));
    HllCtx ctx1 = columnData2->get(1);
    ASSERT_EQ(count, autil::Hyperloglog::hllCtxCount(&ctx1));
}

} // namespace table
