#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/sql/rank/ComparatorCreator.h>
#include <ha3/sql/rank/ColumnComparator.h>
#include <ha3/sql/rank/OneDimColumnComparator.h>
#include <navi/log/NaviLoggerProvider.h>
#include <khronos_table_interface/CommonDefine.h>
#include <khronos_table_interface/DataSeries.hpp>
#include <autil/ConstString.h>

using namespace std;
using namespace khronos;
using namespace testing;
using namespace matchdoc;
using namespace autil;
BEGIN_HA3_NAMESPACE(sql);

class TableUtilTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    void createTable() {
        const auto &docs = getMatchDocs(_allocator, 4);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "id", {1, 2, 3, 4}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<uint32_t>(_allocator, docs, "a", {3, 2, 4, 1}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "name", {"1", "2", "3", "4"}));
        _table.reset(new Table(docs, _allocator));
    }

    void createDataStr(const vector<KHR_TS_TYPE> &timestamps,
                          const vector<KHR_VALUE_TYPE> &values,
                          std::string &dataStr) {
        ASSERT_EQ(timestamps.size(), values.size());
        DataSeriesWriteOnly dataSeries;
        for (size_t i = 0; i < timestamps.size(); i++) {
            dataSeries.append({timestamps[i], values[i]});
        }
        MultiChar mc;
        autil::mem_pool::Pool pool;
        ASSERT_TRUE(dataSeries.serialize(&pool, mc));
        dataStr = {mc.data(), mc.size()};
    }

    void createKhronosDataByDataStr(const std::string &dataStr,
            MultiChar &khronosData) {
        char* buf = MultiValueCreator::createMultiValueBuffer<char>(
                dataStr.data(), dataStr.size(), &_pool);
        khronosData.init(buf);
    }

    void createKhronosTable() {
        const auto &docs = getMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(
                        _allocator, docs, "tagk1", {string("a"), string("b")}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(
                        _allocator, docs, "tagk2", {string("A"), string("B")}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(
                        _allocator, docs, "tagk3", {100, 200}));
        MultiChar khronosData1;
        string dataStr1;
        createDataStr({0, 1, 2}, {0.1, 0.2, 0.3}, dataStr1);
        createKhronosDataByDataStr(dataStr1, khronosData1);

        MultiChar khronosData2;
        string dataStr2;
        createDataStr({3, 4, 5}, {0.2, 0.4, 0.6}, dataStr2);
        createKhronosDataByDataStr(dataStr2, khronosData2);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiChar>(
                        _allocator, docs, "khronos_column", {khronosData1, khronosData2}));
        _table.reset(new Table(docs, _allocator));
    }

    void createKhronosErrorTable() {
        const auto &docs = getMatchDocs(_allocator, 2);
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(
                        _allocator, docs, "tagk1", {string("a"), string("b")}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(
                        _allocator, docs, "tagk2", {string("A"), string("B")}));
        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int32_t>(
                        _allocator, docs, "tagk3", {100, 200}));

        MultiChar khronosData1;
        string dataStr;
        createDataStr({0, 1, 2}, {0.1, 0.2, 0.3}, dataStr);
        createKhronosDataByDataStr(dataStr, khronosData1);

        MultiChar khronosData2;
        createKhronosDataByDataStr("error12error34", khronosData2);

        ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<MultiChar>(
                        _allocator, docs, "khronos_column", {khronosData1, khronosData2}));
        _table.reset(new Table(docs, _allocator));
    }

    string getLastTrace(const navi::NaviLoggerProvider &provider) {
        auto trace = provider.getErrorMessage();
        auto start = trace.rfind('[');
        auto end = trace.rfind(']');
        return trace.substr(start + 1, end - start - 1);
    }
public:
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, TableUtilTest);

void TableUtilTest::setUp() {
}

void TableUtilTest::tearDown() {
}

TEST_F(TableUtilTest, testSort) {
    createTable();
    vector<string> keys = {"b", "a"};
    vector<bool> orders = {false, false};
    ComboComparatorPtr comparator = ComparatorCreator::createComboComparator(
            _table, keys, orders, &_pool);
    TableUtil::sort(_table, comparator.get());
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {1, 3, 4, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "a", {3, 4, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<int64_t>(_table, "b", {1, 1, 2, 2}));
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn(_table, "name", {"1", "3", "4", "2"}));
}

TEST_F(TableUtilTest, testOrder) {
    createTable();
    vector<string> keys = {"b", "a"};
    vector<bool> orders = {true, true};
    ComboComparatorPtr comparator = ComparatorCreator::createComboComparator(
            _table, keys, orders, &_pool);
    TableUtil::sort(_table, comparator.get());
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {2, 4, 3, 1}));
}

TEST_F(TableUtilTest, testOrderMultiChar) {
    createTable();
    vector<string> keys = {"name"};
    vector<bool> orders = {true};
    ComboComparatorPtr comparator = ComparatorCreator::createComboComparator(
            _table, keys, orders, &_pool);
    TableUtil::sort(_table, comparator.get());
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {4, 3, 2, 1}));
}

TEST_F(TableUtilTest, testOrderColumnComparator) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    OneDimColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::sort(_table, &comparator);
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {4, 3, 2, 1}));
}

TEST_F(TableUtilTest, testTopK) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    OneDimColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 1);
    ASSERT_EQ(1, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {4}));
}

TEST_F(TableUtilTest, testTopKWithRows) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    OneDimColumnDescComparator<uint32_t> comparator(columnData);
    vector<Row> rowVec = _table->getRows();
    TableUtil::topK(_table, &comparator, 1, rowVec);
    ASSERT_EQ(1, rowVec.size());
    _table->setRows(rowVec);
    ASSERT_EQ(1, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {4}));
}

TEST_F(TableUtilTest, testTopK2) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    OneDimColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 2);
    OneDimColumnAscComparator<uint32_t> comparator2(columnData);
    TableUtil::sort(_table, &comparator2);
    ASSERT_EQ(2, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {3, 4}));
}

TEST_F(TableUtilTest, testTopKBigLimit) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    OneDimColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 5);
    ASSERT_EQ(4, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {1, 2, 3, 4}));
}

TEST_F(TableUtilTest, testTopKZeroLimit) {
    createTable();
    auto columnData = _table->getColumn("id")->getColumnData<uint32_t>();
    OneDimColumnDescComparator<uint32_t> comparator(columnData);
    TableUtil::topK(_table, &comparator, 0);
    ASSERT_EQ(0, _table->getRowCount());
    ASSERT_EQ(4, _table->getColumnCount());
    ASSERT_NO_FATAL_FAILURE(checkOutputColumn<uint32_t>(_table, "id", {}));
}

TEST_F(TableUtilTest, testCalcHashValue) {
    EXPECT_EQ(1231, TableUtil::calculateHashValue<int32_t>(1231));
    EXPECT_EQ(1231, TableUtil::calculateHashValue<uint32_t>(1231));
    EXPECT_EQ(std::hash<float>{}(1.1), TableUtil::calculateHashValue<float>(1.1));
    {
        string st = "test";
        autil::MultiChar p(autil::MultiValueCreator::createMultiValueBuffer(
                        st.data(), st.size(), &_pool));
        EXPECT_EQ(isearch::util::CityHash64(st.data(), st.size()),
                  TableUtil::calculateHashValue<autil::MultiChar>(p));
    }
    {
        typedef int32_t T;
        vector<T> vec = {1, 2, 3, 4};
        autil::MultiValueType<T> p(autil::MultiValueCreator::createMultiValueBuffer(vec, &_pool));
        EXPECT_EQ(isearch::util::CityHash64(p.getBaseAddress(), p.length()), TableUtil::calculateHashValue<autil::MultiInt32>(p));
    }
    {
        vector<string> vec = {"1", "2", "3", "4"};
        autil::MultiString p(autil::MultiValueCreator::createMultiValueBuffer(vec, &_pool));
        EXPECT_EQ(isearch::util::CityHash64(p.getBaseAddress(), p.length()), TableUtil::calculateHashValue<autil::MultiString>(p));
    }
    {
        vector<int32_t> vec1 = {};
        autil::MultiInt32 p1(autil::MultiValueCreator::createMultiValueBuffer(vec1, &_pool));
        EXPECT_EQ(isearch::util::CityHash64(p1.getBaseAddress(), p1.length()), TableUtil::calculateHashValue<autil::MultiInt32>(p1));

        vector<int64_t> vec2 = {};
        autil::MultiInt64 p2(autil::MultiValueCreator::createMultiValueBuffer(vec2, &_pool));
        EXPECT_EQ(isearch::util::CityHash64(p2.getBaseAddress(), p2.length()), TableUtil::calculateHashValue<autil::MultiInt64>(p2));
        EXPECT_EQ(TableUtil::calculateHashValue<autil::MultiInt32>(p1), TableUtil::calculateHashValue<autil::MultiInt64>(p2));
    }
    {
        vector<int32_t> vec1 = {1};
        autil::MultiInt32 p1(autil::MultiValueCreator::createMultiValueBuffer(vec1, &_pool));
        EXPECT_EQ(isearch::util::CityHash64(p1.getBaseAddress(), p1.length()), TableUtil::calculateHashValue<autil::MultiInt32>(p1));

        vector<int64_t> vec2 = {1};
        autil::MultiInt64 p2(autil::MultiValueCreator::createMultiValueBuffer(vec2, &_pool));
        EXPECT_EQ(isearch::util::CityHash64(p2.getBaseAddress(), p2.length()), TableUtil::calculateHashValue<autil::MultiInt64>(p2));
        EXPECT_TRUE(TableUtil::calculateHashValue<autil::MultiInt32>(p1) != TableUtil::calculateHashValue<autil::MultiInt64>(p2));
    }

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
    TablePtr table(new Table(_poolPtr));
    {
        auto a = TableUtil::declareAndGetColumnData<uint32_t>(table, "a");
        ASSERT_TRUE(nullptr != a);
        ASSERT_EQ("a", table->getColumnName(0));
    }
    {
        auto a = TableUtil::declareAndGetColumnData<uint32_t>(table, "a");
        ASSERT_FALSE(nullptr != a);
    }
    {
        auto a = TableUtil::declareAndGetColumnData<uint32_t>(table, "a", true, false);
        ASSERT_TRUE(nullptr != a);
    }
    {
        auto b = TableUtil::declareAndGetColumnData<string>(table, "b");
        ASSERT_FALSE(nullptr != b);
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

TEST_F(TableUtilTest, testReorder) {
    createTable();
    {
        ASSERT_FALSE(TableUtil::reorderColumn(_table, {}));
    }
    {
        ASSERT_TRUE(TableUtil::reorderColumn(_table, {"id", "name", "b", "a"}));
        ASSERT_EQ("id", _table->getColumnName(0));
        ASSERT_EQ("name", _table->getColumnName(1));
        ASSERT_EQ("b", _table->getColumnName(2));
        ASSERT_EQ("a", _table->getColumnName(3));
    }
    {
        ASSERT_FALSE(TableUtil::reorderColumn(_table, {"id", "name", "b", "b"}));
    }
    {
        ASSERT_FALSE(TableUtil::reorderColumn(_table, {"id", "name", "b", "not exist"}));
    }
}

TEST_F(TableUtilTest, testToJsonString) {
    navi::NaviLoggerProvider provider;
    const auto &docs = getMatchDocs(_allocator, 4);
    ASSERT_NO_FATAL_FAILURE(extendMultiValueMatchDocAllocator<int32_t>(_allocator, docs, "id", {{1, 1}, {2, 2}, {3, 3}, {4, 4}}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<int64_t>(_allocator, docs, "b", {1, 2, 1, 2}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator<bool>(_allocator, docs, "c", {false, false, true, true}));
    ASSERT_NO_FATAL_FAILURE(extendMatchDocAllocator(_allocator, docs, "name", {"1", "2", "3", "4"}));
    _table.reset(new Table(docs, _allocator));
    string ret = TableUtil::toJsonString(_table);
    string expect = R"json({"data":[[[1,1],1,false,"1"],[[2,2],2,false,"2"],[[3,3],1,true,"3"],[[4,4],2,true,"4"]],"column_name":["id","b","c","name"],"column_type":["multi_int32","int64","bool","multi_char"]})json";
    ASSERT_EQ(expect, ret);
    ASSERT_EQ("", getLastTrace(provider));
}

END_HA3_NAMESPACE(sql);
