#include "autil/mem_pool/Pool.h"
#include "table/Table.h"
#include "unittest/unittest.h"

namespace table {

class TableWithoutMatchDocTest : public TESTBASE {
public:
    void setUp() override { _pool = std::make_shared<autil::mem_pool::Pool>(); }

protected:
    std::shared_ptr<autil::mem_pool::Pool> _pool;
};

TEST_F(TableWithoutMatchDocTest, testDeclareColumns) {
    auto table = std::make_unique<Table>(_pool);
    ASSERT_EQ(0, table->getRowCount());
    ASSERT_EQ(0, table->getColumnCount());
    ASSERT_EQ(nullptr, table->getColumn("a"));
    ASSERT_EQ(nullptr, table->getColumn(0));

    auto c1 = table->declareColumn<int32_t>("a");
    ASSERT_TRUE(c1 != nullptr);
    ASSERT_EQ(1, table->getColumnCount());

    auto c2 = table->declareColumn<int32_t>("a");
    ASSERT_TRUE(c2 != nullptr);
    ASSERT_EQ(c1, c2);

    auto c3 = table->declareColumn<std::vector<std::string>>("vec");
    ASSERT_EQ(nullptr, c3);
}

TEST_F(TableWithoutMatchDocTest, testAllocateRows) {
    auto table = std::make_unique<Table>(_pool);
    ASSERT_EQ(0, table->getRowCount());
    ASSERT_EQ(0, table->getColumnCount());

    auto c1 = table->declareColumn<int32_t>("a");
    auto c2 = table->declareColumn<autil::MultiValueType<uint32_t>>("b");
    ASSERT_TRUE(c1 != nullptr);
    ASSERT_TRUE(c2 != nullptr);
    ASSERT_EQ(2, table->getColumnCount());

    auto r = table->allocateRow();
    ASSERT_EQ(0, r.getIndex());
    ASSERT_EQ(1, table->_size);
    ASSERT_EQ(matchdoc::VectorStorage::CHUNKSIZE, table->_capacity);

    table->batchAllocateRow(table->_capacity);
    ASSERT_EQ(matchdoc::VectorStorage::CHUNKSIZE + 1, table->getRowCount());
    ASSERT_EQ(table->getRowCount(), table->_size);
    ASSERT_EQ(matchdoc::VectorStorage::CHUNKSIZE * 2, table->_capacity);

    ColumnData<autil::MultiValueType<uint32_t>> *c2Data = c2->getColumnData<autil::MultiValueType<uint32_t>>();
    ASSERT_TRUE(c2Data != nullptr);

    for (size_t i = 0; i < table->getRowCount(); ++i) {
        auto v = c2Data->get(i);
        ASSERT_EQ(0, v.size());
    }

    {
        std::vector<uint32_t> vec{1, 2, 3};
        c2Data->set(1, vec.data(), vec.size());
    }
    auto v = c2Data->get(1);
    ASSERT_EQ(3, v.size());
    ASSERT_EQ(1, v[0]);
    ASSERT_EQ(2, v[1]);
    ASSERT_EQ(3, v[2]);

    for (size_t i = 0; i < 101; ++i) {
        table->markDeleteRow(i);
    }
    table->deleteRows();
    ASSERT_EQ(matchdoc::VectorStorage::CHUNKSIZE + 1 - 101, table->getRowCount());
    auto r2 = table->allocateRow();
    ASSERT_EQ(table->_size - 1, r2.getIndex());
    ASSERT_GT(r2.getIndex(), table->getRowCount());

    auto c3 = table->declareColumn<autil::MultiValueType<float>>("c");
    ASSERT_TRUE(c3 != nullptr);
}

TEST_F(TableWithoutMatchDocTest, testCompact) {
    auto table = std::make_unique<Table>(_pool);
    auto c = table->declareColumn<int64_t>("c");
    ASSERT_TRUE(c != nullptr);

    table->batchAllocateRow(1024 * 10);
    ASSERT_EQ(1024 * 10 * sizeof(int64_t), table->usedBytes());

    auto *data = c->getColumnData<int64_t>();
    for (size_t i = 0; i < 8 * 1024; ++i) {
        table->markDeleteRow(i);
    }
    for (size_t i = 8 * 1024; i < 10 * 1024; ++i) {
        data->set(i, i);
    }
    table->deleteRows();
    ASSERT_FALSE(table->compact());

    ASSERT_TRUE(table->compact(1024));
    ASSERT_EQ(2 * 1024, table->getRowCount());
    ASSERT_EQ(2 * 1024, table->_size);
    ASSERT_EQ(2 * 1024, table->_capacity);

    c = table->getColumn("c");
    ASSERT_TRUE(c != nullptr);
    data = c->getColumnData<int64_t>();
    ASSERT_TRUE(data != nullptr);
    ASSERT_EQ(&table->_rows, data->_rows);
    ASSERT_EQ(8 * 1024, data->get(0));
    ASSERT_EQ(8 * 1024 + 100, data->get(100));
}

} // namespace table
