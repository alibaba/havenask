#include "table/TableUtil.h"

#include <functional>
#include <memory>
#include <string>
#include <vector>

#include "autil/HashUtil.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/Trait.h"
#include "matchdoc/ValueType.h"
#include "table/Column.h"
#include "table/ColumnData.h"
#include "table/ComboComparator.h"
#include "table/ComparatorCreator.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

namespace autil {
class HllCtx;
} // namespace autil

struct SomeType {};

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;
namespace table {

class TableUtilTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void createTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
        ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(_allocator, docs, "name", {"1", "2", "3", "4"}));
        _table = Table::fromMatchDocs(docs, _allocator);
        ASSERT_TRUE(_table);
    }

    // TODO(xinfei.sxf) use other function to get trace?
    /*
    string getLastTrace(const navi::NaviLoggerProvider &provider) {
        auto trace = provider.getErrorMessage();
        auto start = trace.rfind('[');
        auto end = trace.rfind(']');
        return trace.substr(start + 1, end - start - 1);
    }
    */
public:
    autil::mem_pool::Pool _pool;
    MatchDocUtil _matchDocUtil;
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;

private:
};

void TableUtilTest::setUp() {}

void TableUtilTest::tearDown() {}

TEST_F(TableUtilTest, testSort) {
    createTable();
    vector<string> keys = {"b", "a"};
    vector<bool> orders = {false, false};
    ComboComparatorPtr comparator = ComparatorCreator::createComboComparator(_table, keys, orders, &_pool);
    TableUtil::sort(_table, comparator.get());
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {1, 3, 4, 2}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "a", {3, 4, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<int64_t>(_table, "b", {1, 1, 2, 2}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(_table, "name", {"1", "3", "4", "2"}));
}

TEST_F(TableUtilTest, testOrder) {
    createTable();
    vector<string> keys = {"b", "a"};
    vector<bool> orders = {true, true};
    ComboComparatorPtr comparator = ComparatorCreator::createComboComparator(_table, keys, orders, &_pool);
    TableUtil::sort(_table, comparator.get());
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {2, 4, 3, 1}));
}

TEST_F(TableUtilTest, testOrderMultiChar) {
    createTable();
    vector<string> keys = {"name"};
    vector<bool> orders = {true};
    ComboComparatorPtr comparator = ComparatorCreator::createComboComparator(_table, keys, orders, &_pool);
    TableUtil::sort(_table, comparator.get());
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {4, 3, 2, 1}));
}

TEST_F(TableUtilTest, testOrderColumnComparator) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    ColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::sort(_table, &comparator);
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {4, 3, 2, 1}));
}

TEST_F(TableUtilTest, testTopK) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    ColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 1);
    ASSERT_EQ(1, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {4}));
}

TEST_F(TableUtilTest, testTopKWithRows) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    ColumnDescComparator<uint32_t> comparator(columnData);
    vector<Row> rowVec = _table->getRows();
    TableUtil::topK(_table, &comparator, 1, rowVec);
    ASSERT_EQ(1, rowVec.size());
    _table->setRows(std::move(rowVec));
    ASSERT_EQ(1, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {4}));
}

TEST_F(TableUtilTest, testTopK2) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    ColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 2);
    ColumnAscComparator<uint32_t> comparator2(columnData);
    TableUtil::sort(_table, &comparator2);
    ASSERT_EQ(2, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {3, 4}));
}

TEST_F(TableUtilTest, testTopKBigLimit) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    ColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 5);
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {1, 2, 3, 4}));
}

TEST_F(TableUtilTest, testTopKZeroLimit) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    ColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 0);
    ASSERT_EQ(0, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {}));
}

TEST_F(TableUtilTest, testTopKReserve) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    ColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 1, true);
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn<uint32_t>(_table, "id", {4, 3, 1, 2}));
}

TEST_F(TableUtilTest, testGetColumnData) {
    createTable();
    {
        auto a = TableUtil::getColumnData<uint32_t>(_table, "a");
        ASSERT_TRUE(nullptr != a);
        ASSERT_EQ(3, a->get(0));
    }
    {
        auto x = TableUtil::getColumnData<uint32_t>(_table, "x");
        ASSERT_TRUE(nullptr == x);
    }
    {
        auto a = TableUtil::getColumnData<int32_t>(_table, "a");
        ASSERT_TRUE(nullptr == a);
    }
}

TEST_F(TableUtilTest, testDeclareColumn) {
    std::shared_ptr<autil::mem_pool::Pool> pool(new autil::mem_pool::Pool());
    TablePtr table(new Table(pool));
    {
        auto a = TableUtil::declareAndGetColumnData<uint32_t>(table, "a");
        ASSERT_TRUE(nullptr != a);
        ASSERT_EQ("a", table->getColumn(0)->getName());
    }
    {
        auto a = TableUtil::declareAndGetColumnData<uint32_t>(table, "a");
        ASSERT_FALSE(nullptr != a);
    }
    {
        auto a = TableUtil::declareAndGetColumnData<uint32_t>(table, "a", false);
        ASSERT_TRUE(nullptr != a);
    }
    {
        auto b = TableUtil::declareAndGetColumnData<string>(table, "b");
        ASSERT_TRUE(nullptr == b);
    }
}

TEST_F(TableUtilTest, testValueTypeToString) {
    ASSERT_EQ("int8", TableUtil::valueTypeToString(ValueTypeHelper<int8_t>::getValueType()));
    ASSERT_EQ("int16", TableUtil::valueTypeToString(ValueTypeHelper<int16_t>::getValueType()));
    ASSERT_EQ("int32", TableUtil::valueTypeToString(ValueTypeHelper<int32_t>::getValueType()));
    ASSERT_EQ("int64", TableUtil::valueTypeToString(ValueTypeHelper<int64_t>::getValueType()));
    ASSERT_EQ("uint8", TableUtil::valueTypeToString(ValueTypeHelper<uint8_t>::getValueType()));
    ASSERT_EQ("uint16", TableUtil::valueTypeToString(ValueTypeHelper<uint16_t>::getValueType()));
    ASSERT_EQ("uint32", TableUtil::valueTypeToString(ValueTypeHelper<uint32_t>::getValueType()));
    ASSERT_EQ("uint64", TableUtil::valueTypeToString(ValueTypeHelper<uint64_t>::getValueType()));
    ASSERT_EQ("float", TableUtil::valueTypeToString(ValueTypeHelper<float>::getValueType()));
    ASSERT_EQ("double", TableUtil::valueTypeToString(ValueTypeHelper<double>::getValueType()));
    ASSERT_EQ("multi_char", TableUtil::valueTypeToString(ValueTypeHelper<string>::getValueType()));
    ASSERT_EQ("multi_char", TableUtil::valueTypeToString(ValueTypeHelper<autil::MultiChar>::getValueType()));
    ASSERT_EQ("multi_int32", TableUtil::valueTypeToString(ValueTypeHelper<autil::MultiInt32>::getValueType()));
    ASSERT_EQ("multi_multi_char", TableUtil::valueTypeToString(ValueTypeHelper<autil::MultiString>::getValueType()));
    ASSERT_EQ("unknown_type", TableUtil::valueTypeToString(ValueType()));
    ASSERT_EQ("unknown_type", TableUtil::valueTypeToString(ValueTypeHelper<autil::HllCtx>::getValueType()));
}

TEST_F(TableUtilTest, testToJsonString) {
    const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 4);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
        _allocator, docs, "id", {{1, 1}, {2, 2}, {3, 3}, {4, 4}}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMatchDocAllocator<bool>(_allocator, docs, "c", {false, false, true, true}));
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(_allocator, docs, "name", {"1", "2", "3", "4"}));
    _table = Table::fromMatchDocs(docs, _allocator);
    ASSERT_TRUE(_table);
    string ret = TableUtil::toJsonString(_table);
    string expect =
        R"json({"data":[[[1,1],1,false,"1"],[[2,2],2,false,"2"],[[3,3],1,true,"3"],[[4,4],2,true,"4"]],"column_name":["id","b","c","name"],"column_type":["multi_int32","int64","bool","multi_char"]})json";
    ASSERT_EQ(expect, ret);
}

TEST_F(TableUtilTest, testCalcHash) {
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {1, 2, 3}));
    _table = Table::fromMatchDocs(docs, _allocator);
    ASSERT_TRUE(_table);
    vector<string> groupKeyVec = {"a"};
    vector<size_t> result;
    ASSERT_TRUE(TableUtil::calculateGroupKeyHash(_table, groupKeyVec, result));
    auto data = _table->getColumn(groupKeyVec[0])->getColumnData<uint32_t>()->get(0);
    size_t expectHashValue = autil::HashUtil::calculateHashValue<uint32_t>(data);
    ASSERT_EQ(expectHashValue, result[0]);
    result.clear();
    vector<Row> rows;
    rows.push_back(MatchDoc(0, 0));
    ASSERT_TRUE(TableUtil::calculateGroupKeyHashWithSpecificRow(_table, groupKeyVec, rows, result));
    ASSERT_EQ(expectHashValue, result[0]);
}

TEST_F(TableUtilTest, testCalcHashMultiType) {
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 3);
    ASSERT_NO_FATAL_FAILURE(
        _matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(_allocator, docs, "a", {{1}, {2}, {3}}));
    _table = Table::fromMatchDocs(docs, _allocator);
    ASSERT_TRUE(_table);
    vector<string> groupKeyVec = {"a"};
    vector<size_t> result;
    ASSERT_TRUE(TableUtil::calculateGroupKeyHash(_table, groupKeyVec, result));
    auto column = _table->getColumn("a");
    ASSERT_NE(nullptr, column);
    auto *columnData = column->getColumnData<MultiInt32>();
    ASSERT_NE(nullptr, columnData);
    ASSERT_EQ(3, result.size());
    ASSERT_EQ(autil::HashUtil::calculateHashValue(columnData->get(0)), result[0]);
    ASSERT_EQ(autil::HashUtil::calculateHashValue(columnData->get(1)), result[1]);
    ASSERT_EQ(autil::HashUtil::calculateHashValue(columnData->get(2)), result[2]);
}

TEST_F(TableUtilTest, testCalcHashMultiChar) {
    vector<MatchDoc> docs = _matchDocUtil.createMatchDocs(_allocator, 1);
    ASSERT_NO_FATAL_FAILURE(_matchDocUtil.extendMatchDocAllocator(_allocator, docs, "a", {"hello"}));
    _table = Table::fromMatchDocs(docs, _allocator);
    ASSERT_TRUE(_table);
    vector<string> groupKeyVec = {"a"};
    vector<size_t> result;
    ASSERT_TRUE(TableUtil::calculateGroupKeyHash(_table, groupKeyVec, result));
}

} // namespace table
