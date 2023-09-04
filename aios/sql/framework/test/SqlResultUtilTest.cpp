#include "sql/framework/SqlResultUtil.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <ext/alloc_traits.h>
#include <flatbuffers/flatbuffers.h>
#include <flatbuffers/stl_emulation.h>
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include "autil/Log.h"
#include "autil/MultiValueType.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/Pool.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/proto/SqlResult_generated.h"
#include "ha3/proto/TwoDimTable_generated.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
#include "matchdoc/ValueType.h"
#include "navi/util/NaviTestPool.h"
#include "sql/data/ErrorResult.h"
#include "sql/framework/QrsSessionSqlResult.h"
#include "table/Column.h"
#include "table/Row.h"
#include "table/Table.h"
#include "table/TableUtil.h"
#include "table/test/MatchDocUtil.h"
#include "table/test/TableTestUtil.h"
#include "unittest/unittest.h"

namespace sql {
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByDouble_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByFloat_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByInt16_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByInt32_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByInt64_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByInt8_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiDouble_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiFloat_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiInt16_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiInt32_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiInt64_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiInt8_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiUInt16_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiUInt32_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiUInt64_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByMultiUInt8_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByUInt16_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByUInt32_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByUInt64_Test;
class SqlResultUtilTest_testCreateAndFromSqlTableColumnByUInt8_Test;
} // namespace sql

namespace table {
template <typename T>
class ColumnData;
} // namespace table

using namespace std;
using namespace testing;
using namespace table;
using namespace matchdoc;
using namespace autil;
using namespace flatbuffers;
using namespace isearch::fbs;
using namespace isearch;

namespace sql {

class SqlResultUtilTest : public TESTBASE {
public:
    void setUp();
    void tearDown();

public:
    void createNoDataTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 0);
        _matchDocUtil.extendMatchDocAllocator<uint8_t>(_allocator, docs, "fuint8", {});
        _matchDocUtil.extendMatchDocAllocator<uint16_t>(_allocator, docs, "fuint16", {});
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fuint32", {});
        _matchDocUtil.extendMatchDocAllocator<uint64_t>(_allocator, docs, "fuint64", {});
        _matchDocUtil.extendMatchDocAllocator<int8_t>(_allocator, docs, "fint8", {});
        _matchDocUtil.extendMatchDocAllocator<int16_t>(_allocator, docs, "fint16", {});
        _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, docs, "fint32", {});
        _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "fint64", {});
        _table.reset(new table::Table(docs, _allocator));
    }

    void createSingleNumericTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 4);
        _matchDocUtil.extendMatchDocAllocator<uint8_t>(_allocator, docs, "fuint8", {1, 2, 3, 4});
        _matchDocUtil.extendMatchDocAllocator<uint16_t>(_allocator, docs, "fuint16", {1, 2, 3, 4});
        _matchDocUtil.extendMatchDocAllocator<uint32_t>(_allocator, docs, "fuint32", {1, 2, 3, 4});
        _matchDocUtil.extendMatchDocAllocator<uint64_t>(_allocator, docs, "fuint64", {1, 2, 3, 4});
        _matchDocUtil.extendMatchDocAllocator<int8_t>(_allocator, docs, "fint8", {1, 2, 3, 4});
        _matchDocUtil.extendMatchDocAllocator<int16_t>(_allocator, docs, "fint16", {1, 2, 3, 4});
        _matchDocUtil.extendMatchDocAllocator<int32_t>(_allocator, docs, "fint32", {1, 2, 3, 4});
        _matchDocUtil.extendMatchDocAllocator<int64_t>(_allocator, docs, "fint64", {1, 2, 3, 4});
        _table.reset(new table::Table(docs, _allocator));
    }

    void createMultiNumericTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 4);
        _matchDocUtil.extendMultiValueMatchDocAllocator<uint8_t>(
            _allocator, docs, "fmuint8", {{1}, {2}, {3}, {4}});
        _matchDocUtil.extendMultiValueMatchDocAllocator<uint16_t>(
            _allocator, docs, "fmuint16", {{1}, {2}, {3}, {4}});
        _matchDocUtil.extendMultiValueMatchDocAllocator<uint32_t>(
            _allocator, docs, "fmuint32", {{1}, {2}, {3}, {4}});
        _matchDocUtil.extendMultiValueMatchDocAllocator<uint64_t>(
            _allocator, docs, "fmuint64", {{1}, {2}, {3}, {4}});
        _matchDocUtil.extendMultiValueMatchDocAllocator<int8_t>(
            _allocator, docs, "fmint8", {{1}, {2}, {3}, {4}});
        _matchDocUtil.extendMultiValueMatchDocAllocator<int16_t>(
            _allocator, docs, "fmint16", {{1}, {2}, {3}, {4}});
        _matchDocUtil.extendMultiValueMatchDocAllocator<int32_t>(
            _allocator, docs, "fmint32", {{1}, {2}, {3}, {4}});
        _matchDocUtil.extendMultiValueMatchDocAllocator<int64_t>(
            _allocator, docs, "fmint64", {{1}, {2}, {3}, {4}});

        _table.reset(new table::Table(docs, _allocator));
    }

    void createStringTable() {
        const auto &docs = _matchDocUtil.createMatchDocs(_allocator, 4);
        _matchDocUtil.extendMultiValueMatchDocAllocator<string>(
            _allocator,
            docs,
            "fmstring",
            {{"abc", "def", "g"}, {"hij", "klm", "n"}, {"opq", "rst"}, {"uvw", "xyz"}});
        _matchDocUtil.extendMatchDocAllocator(
            _allocator, docs, "fstring", {"abcdefg", "hijklmn", "opqrst", "uvwxyz"});
        _table.reset(new table::Table(docs, _allocator));
    }

    template <typename T>
    TablePtr createSingleColumnTableByVec(const string &columnName, const vector<T> &values) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = _matchDocUtil.createMatchDocs(allocator, values.size());
        _matchDocUtil.extendMatchDocAllocator<T>(allocator, docs, columnName, values);
        return TablePtr(new table::Table(docs, allocator));
    }

    TablePtr createSingleStringColumnTableByVec(const string &columnName,
                                                const vector<string> &values) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = _matchDocUtil.createMatchDocs(allocator, values.size());
        _matchDocUtil.extendMatchDocAllocator(allocator, docs, columnName, values);
        return TablePtr(new table::Table(docs, allocator));
    }

    template <typename T>
    TablePtr createMSingleColumnTableByVec(const string &columnName,
                                           const vector<vector<T>> &values) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = _matchDocUtil.createMatchDocs(allocator, values.size());
        _matchDocUtil.extendMultiValueMatchDocAllocator<T>(allocator, docs, columnName, values);
        return TablePtr(new table::Table(docs, allocator));
    }

public:
    std::shared_ptr<autil::mem_pool::Pool> _poolPtr;
    table::MatchDocUtil _matchDocUtil;
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;

private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(sql, SqlResultUtilTest);

void SqlResultUtilTest::setUp() {
    _poolPtr.reset(new autil::mem_pool::Pool());
}

void SqlResultUtilTest::tearDown() {}

#define DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(fieldType, columnType)                        \
    template <>                                                                                    \
    void checkColumnValue<fieldType>(const isearch::fbs::Column *fbsColumn,                        \
                                     const std::vector<fieldType> &expectedValues) {               \
        ASSERT_EQ(ColumnType_##columnType##Column, fbsColumn->value_type());                       \
        const auto *column = fbsColumn->value_as_##columnType##Column();                           \
        ASSERT_TRUE(column != nullptr);                                                            \
        const auto *columnValue = column->value();                                                 \
        ASSERT_EQ(expectedValues.size(), columnValue->size());                                     \
        for (size_t i = 0; i < expectedValues.size(); i++) {                                       \
            ASSERT_EQ(expectedValues[i], columnValue->Get(i));                                     \
        }                                                                                          \
    }

template <typename T>
void checkColumnValue(const isearch::fbs::Column *fbsColumn, const std::vector<T> &expectedValues)
    = delete;
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(int8_t, Int8);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(int16_t, Int16);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(int32_t, Int32);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(int64_t, Int64);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint8_t, UInt8);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint16_t, UInt16);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint32_t, UInt32);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint64_t, UInt64);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(float, Float);
DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(double, Double);

template <>
void checkColumnValue<string>(const isearch::fbs::Column *fbsColumn,
                              const std::vector<string> &expectedValues) {
    ASSERT_EQ(ColumnType_StringColumn, fbsColumn->value_type());
    const auto *column = fbsColumn->value_as_StringColumn();
    ASSERT_TRUE(column != nullptr);
    const auto *columnValue = column->value();
    ASSERT_EQ(expectedValues.size(), columnValue->size());
    for (size_t i = 0; i < expectedValues.size(); i++) {
        ASSERT_EQ(expectedValues[i], columnValue->Get(i)->str());
    }
}
void checkByteStringColumnValue(const isearch::fbs::Column *fbsColumn,
                                const std::vector<string> &expectedValues) {
    ASSERT_EQ(ColumnType_ByteStringColumn, fbsColumn->value_type());
    const auto *column = fbsColumn->value_as_ByteStringColumn();
    ASSERT_TRUE(column != nullptr);
    const auto *columnValue = column->value();
    ASSERT_EQ(expectedValues.size(), columnValue->size());
    for (size_t i = 0; i < expectedValues.size(); i++) {
        auto byteVal = columnValue->Get(i)->value();
        string columnVal((char *)byteVal->Data(), byteVal->size());
        ASSERT_EQ(expectedValues[i], columnVal);
    }
}

#define DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(fbType, fieldType)             \
    TEST_F(SqlResultUtilTest, testCreateAndFromSqlTableColumnBy##fbType) {                         \
        string columnName = "f" #fieldType;                                                        \
        fieldType maxValue = std::numeric_limits<fieldType>::max();                                \
        vector<fieldType> expectedValue = {maxValue, --maxValue, --maxValue, --maxValue};          \
        table::TablePtr table = createSingleColumnTableByVec(columnName, expectedValue);           \
        FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());                             \
        FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);                                    \
        ColumnData<fieldType> *column                                                              \
            = table::TableUtil::getColumnData<fieldType>(table, columnName);                       \
        ASSERT_TRUE(column != nullptr);                                                            \
        vector<Offset<isearch::fbs::Column>> fbsColumnVec;                                         \
        SqlResultUtil().CreateSqlTableColumnBy##fbType(columnName, column, fbb, fbsColumnVec);     \
        ASSERT_EQ(1, fbsColumnVec.size());                                                         \
        Offset<isearch::fbs::Column> fbsColumnOffset = fbsColumnVec[0];                            \
        fbb.Finish(fbsColumnOffset);                                                               \
        char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());                             \
        const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data);               \
        ASSERT_NE(nullptr, fbsColumn);                                                             \
        const flatbuffers::String *testColumnName = fbsColumn->name();                             \
        ASSERT_TRUE(testColumnName != nullptr);                                                    \
        ASSERT_EQ(columnName, testColumnName->str());                                              \
        checkColumnValue<fieldType>(fbsColumn, expectedValue);                                     \
        ASSERT_FALSE(HasFatalFailure());                                                           \
        {                                                                                          \
            TablePtr outputTable;                                                                  \
            auto asanPool = std::make_shared<autil::mem_pool::PoolAsan>();                         \
            outputTable.reset(new table::Table(asanPool));                                         \
            outputTable->batchAllocateRow(table->getRowCount());                                   \
            auto *column = outputTable->declareColumn<fieldType>("a");                             \
            ASSERT_NE(nullptr, column);                                                            \
            auto *columnData = column->getColumnData<fieldType>();                                 \
            ASSERT_NE(nullptr, columnData);                                                        \
            ASSERT_TRUE(SqlResultUtil().FromSqlTableColumnBy##fbType(                              \
                outputTable->getDataPool(), fbsColumn, columnData));                               \
            TableTestUtil::checkOutputColumn<fieldType>(outputTable, "a", expectedValue);          \
            ASSERT_FALSE(HasFatalFailure());                                                       \
        }                                                                                          \
    }

DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(Int8, int8_t)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(Int16, int16_t)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(Int32, int32_t)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(Int64, int64_t)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(UInt8, uint8_t)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(UInt16, uint16_t)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(UInt32, uint32_t)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(UInt64, uint64_t)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(Float, float)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(Double, double)

template <typename T>
void checkColumnMultiValue(const isearch::fbs::Column *fbsColumn,
                           const std::vector<vector<T>> &expectedValues)
    = delete;

#define DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(fieldType, columnType)                         \
    template <>                                                                                    \
    void checkColumnMultiValue(const isearch::fbs::Column *fbsColumn,                              \
                               const vector<vector<fieldType>> &expectedValues) {                  \
        ASSERT_EQ(ColumnType_Multi##columnType##Column, fbsColumn->value_type());                  \
        const auto *column = fbsColumn->value_as_Multi##columnType##Column();                      \
        ASSERT_TRUE(column != nullptr);                                                            \
        const auto *columnValue = column->value();                                                 \
        ASSERT_EQ(expectedValues.size(), columnValue->size());                                     \
        for (size_t i = 0; i < expectedValues.size(); i++) {                                       \
            const Vector<fieldType> *valueVec = columnValue->Get(i)->value();                      \
            ASSERT_TRUE(valueVec != nullptr);                                                      \
            ASSERT_EQ(expectedValues[i].size(), valueVec->size());                                 \
            for (size_t j = 0; j < expectedValues[i].size(); j++) {                                \
                ASSERT_EQ(expectedValues[i][j], valueVec->Get(j));                                 \
            }                                                                                      \
        }                                                                                          \
    }

DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(int8_t, Int8);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(int16_t, Int16);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(int32_t, Int32);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(int64_t, Int64);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint8_t, UInt8);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint16_t, UInt16);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint32_t, UInt32);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(uint64_t, UInt64);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(float, Float);
DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(double, Double);

template <>
void checkColumnMultiValue(const isearch::fbs::Column *fbsColumn,
                           const vector<vector<string>> &expectedValues) {
    ASSERT_EQ(ColumnType_MultiStringColumn, fbsColumn->value_type());
    const auto *column = fbsColumn->value_as_MultiStringColumn();
    ASSERT_TRUE(column != nullptr);
    const auto *columnValue = column->value();
    ASSERT_EQ(expectedValues.size(), columnValue->size());
    for (size_t i = 0; i < expectedValues.size(); i++) {
        const Vector<flatbuffers::Offset<flatbuffers::String>> *valueVec
            = columnValue->Get(i)->value();
        ASSERT_TRUE(valueVec != nullptr);
        ASSERT_EQ(expectedValues[i].size(), valueVec->size());
        for (size_t j = 0; j < expectedValues[i].size(); j++) {
            ASSERT_EQ(expectedValues[i][j], valueVec->Get(j)->str());
        }
    }
}

void checkMultiByteStringColumnMultiValue(const isearch::fbs::Column *fbsColumn,
                                          const vector<vector<string>> &expectedValues) {
    ASSERT_EQ(ColumnType_MultiByteStringColumn, fbsColumn->value_type());
    const auto *column = fbsColumn->value_as_MultiByteStringColumn();
    ASSERT_TRUE(column != nullptr);
    const auto *columnValue = column->value();
    ASSERT_EQ(expectedValues.size(), columnValue->size());
    for (size_t i = 0; i < expectedValues.size(); i++) {
        const Vector<flatbuffers::Offset<isearch::fbs::ByteString>> *valueVec
            = columnValue->Get(i)->value();
        ASSERT_TRUE(valueVec != nullptr);
        ASSERT_EQ(expectedValues[i].size(), valueVec->size());
        for (size_t j = 0; j < expectedValues[i].size(); j++) {
            auto byteVal = valueVec->Get(j)->value();
            string val((char *)byteVal->Data(), byteVal->size());
            ASSERT_EQ(expectedValues[i][j], val);
        }
    }
}

#define DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(fbType, mfieldType)             \
    TEST_F(SqlResultUtilTest, testCreateAndFromSqlTableColumnByMulti##fbType) {                    \
        string columnName = "fm" #mfieldType;                                                      \
        typedef mfieldType::type fieldType;                                                        \
        fieldType maxValue = std::numeric_limits<fieldType>::max();                                \
        vector<vector<fieldType>> expectedValue = {{0, 1, 2},                                      \
                                                   {3, 4, 5},                                      \
                                                   {maxValue, --maxValue, --maxValue},             \
                                                   {--maxValue, --maxValue, --maxValue}};          \
        table::TablePtr table = createMSingleColumnTableByVec(columnName, expectedValue);          \
        FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());                             \
        FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);                                    \
        ColumnData<mfieldType> *column                                                             \
            = table::TableUtil::getColumnData<mfieldType>(table, columnName);                      \
        ASSERT_TRUE(column != nullptr);                                                            \
        vector<Offset<isearch::fbs::Column>> fbsColumnVec;                                         \
        SqlResultUtil().CreateSqlTableColumnByMulti##fbType(                                       \
            columnName, column, fbb, fbsColumnVec);                                                \
        ASSERT_EQ(1, fbsColumnVec.size());                                                         \
        Offset<isearch::fbs::Column> fbsColumnOffset = fbsColumnVec[0];                            \
        fbb.Finish(fbsColumnOffset);                                                               \
        char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());                             \
        const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data);               \
        ASSERT_NE(nullptr, fbsColumn);                                                             \
        const flatbuffers::String *testColumnName = fbsColumn->name();                             \
        ASSERT_TRUE(testColumnName != nullptr);                                                    \
        ASSERT_EQ(columnName, testColumnName->str());                                              \
        checkColumnMultiValue<fieldType>(fbsColumn, expectedValue);                                \
        ASSERT_FALSE(HasFatalFailure());                                                           \
        {                                                                                          \
            TablePtr outputTable;                                                                  \
            auto asanPool = std::make_shared<autil::mem_pool::PoolAsan>();                         \
            outputTable.reset(new table::Table(asanPool));                                         \
            outputTable->batchAllocateRow(table->getRowCount());                                   \
            auto *column = outputTable->declareColumn<mfieldType>("a");                            \
            ASSERT_NE(nullptr, column);                                                            \
            auto *columnData = column->getColumnData<mfieldType>();                                \
            ASSERT_NE(nullptr, columnData);                                                        \
            ASSERT_TRUE(SqlResultUtil().FromSqlTableColumnByMulti##fbType(                         \
                outputTable->getDataPool(), fbsColumn, columnData));                               \
            TableTestUtil::checkOutputMultiColumn<mfieldType::type>(                               \
                outputTable, "a", expectedValue);                                                  \
            ASSERT_FALSE(HasFatalFailure());                                                       \
        }                                                                                          \
    }

DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(Int8, autil::MultiInt8)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(Int16, autil::MultiInt16)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(Int32, autil::MultiInt32)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(Int64, autil::MultiInt64)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(UInt8, autil::MultiUInt8)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(UInt16, autil::MultiUInt16)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(UInt32, autil::MultiUInt32)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(UInt64, autil::MultiUInt64)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(Float, autil::MultiFloat)
DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(Double, autil::MultiDouble)

TEST_F(SqlResultUtilTest, testCreateAndFromSqlTableColumnByString) {
    string columnName = "fstring";
    vector<string> expectedValue = {"abcdefg", "hijklmn", "opqrst", "uvwxyz"};
    table::TablePtr table = createSingleStringColumnTableByVec(columnName, expectedValue);
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    auto column = table->getColumn(columnName);
    ASSERT_TRUE(column != nullptr);
    vector<Offset<isearch::fbs::Column>> fbsColumnVec;
    SqlResultUtil().CreateSqlTableColumnByString(columnName, column, fbb, fbsColumnVec);
    ASSERT_EQ(1, fbsColumnVec.size());
    auto fbsColumnOffset = fbsColumnVec[0];
    fbb.Finish(fbsColumnOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data);
    ASSERT_NE(nullptr, fbsColumn);
    const flatbuffers::String *testColumnName = fbsColumn->name();
    ASSERT_TRUE(testColumnName != nullptr);
    ASSERT_EQ(columnName, testColumnName->str());
    ASSERT_NO_FATAL_FAILURE(checkColumnValue<string>(fbsColumn, expectedValue));
    {
        TablePtr outputTable;
        auto asanPool = std::make_shared<autil::mem_pool::PoolAsan>();
        outputTable.reset(new table::Table(asanPool));
        outputTable->batchAllocateRow(table->getRowCount());
        auto *column = outputTable->declareColumn<autil::MultiChar>("a");
        ASSERT_NE(nullptr, column);
        auto *columnData = column->getColumnData<autil::MultiChar>();
        ASSERT_NE(nullptr, columnData);
        ASSERT_TRUE(SqlResultUtil().FromSqlTableColumnByString(
            outputTable->getDataPool(), fbsColumn, columnData));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(outputTable, "a", expectedValue));
    }
}

TEST_F(SqlResultUtilTest, testCreateAndFromSqlTableColumnByMultiString) {
    string columnName = "fmstring";
    vector<vector<string>> expectedValue
        = {{"abc", "def", "g"}, {"hij", "klm", "n"}, {"opq", "rst"}, {"uvw", "xyz"}};
    table::TablePtr table = createMSingleColumnTableByVec(columnName, expectedValue);
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ColumnData<autil::MultiString> *columnData
        = TableUtil::getColumnData<autil::MultiString>(table, columnName);
    ASSERT_TRUE(columnData != nullptr);

    vector<Offset<isearch::fbs::Column>> fbsColumnVec;
    SqlResultUtil().CreateSqlTableColumnByMultiString(columnName, columnData, fbb, fbsColumnVec);
    ASSERT_EQ(1, fbsColumnVec.size());
    auto fbsColumnOffset = fbsColumnVec[0];
    fbb.Finish(fbsColumnOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data);
    ASSERT_NE(nullptr, fbsColumn);
    const flatbuffers::String *testColumnName = fbsColumn->name();
    ASSERT_TRUE(testColumnName != nullptr);
    ASSERT_EQ(columnName, testColumnName->str());
    ASSERT_NO_FATAL_FAILURE(checkColumnMultiValue<string>(fbsColumn, expectedValue));
    {
        TablePtr outputTable;
        auto asanPool = std::make_shared<autil::mem_pool::PoolAsan>();
        outputTable.reset(new table::Table(asanPool));
        outputTable->batchAllocateRow(table->getRowCount());
        auto *column = outputTable->declareColumn<autil::MultiString>("a");
        ASSERT_NE(nullptr, column);
        auto *columnData = column->getColumnData<autil::MultiString>();
        ASSERT_NE(nullptr, columnData);
        ASSERT_TRUE(SqlResultUtil().FromSqlTableColumnByMultiString(
            outputTable->getDataPool(), fbsColumn, columnData));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputMultiColumn(outputTable, "a", expectedValue));
    }
}

TEST_F(SqlResultUtilTest, testCreateAndFromSqlTableColumnByByteString) {
    string columnName = "fstring";
    vector<string> expectedValue = {"abcdefg", "hijklmn", "opqrst", "uvwxyz"};
    table::TablePtr table = createSingleStringColumnTableByVec(columnName, expectedValue);
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    auto column = table->getColumn(columnName);
    ASSERT_TRUE(column != nullptr);
    vector<Offset<isearch::fbs::Column>> fbsColumnVec;
    SqlResultUtil(true).CreateSqlTableColumnByByteString(columnName, column, fbb, fbsColumnVec);
    ASSERT_EQ(1, fbsColumnVec.size());
    auto fbsColumnOffset = fbsColumnVec[0];
    fbb.Finish(fbsColumnOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data);
    const flatbuffers::String *testColumnName = fbsColumn->name();
    ASSERT_TRUE(testColumnName != nullptr);
    ASSERT_EQ(columnName, testColumnName->str());
    ASSERT_NO_FATAL_FAILURE(checkByteStringColumnValue(fbsColumn, expectedValue));
    {
        TablePtr outputTable;
        auto asanPool = std::make_shared<autil::mem_pool::PoolAsan>();
        outputTable.reset(new table::Table(asanPool));
        outputTable->batchAllocateRow(table->getRowCount());
        auto *column = outputTable->declareColumn<autil::MultiChar>("a");
        ASSERT_NE(nullptr, column);
        auto *columnData = column->getColumnData<autil::MultiChar>();
        ASSERT_NE(nullptr, columnData);
        ASSERT_TRUE(SqlResultUtil().FromSqlTableColumnByByteString(
            outputTable->getDataPool(), fbsColumn, columnData));
        ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(outputTable, "a", expectedValue));
    }
}

TEST_F(SqlResultUtilTest, testCreateAndFromSqlTableColumnByMultiByteString) {
    string columnName = "fmstring";
    vector<vector<string>> expectedValue
        = {{"abc", "def", "g"}, {"hij", "klm", "n"}, {"opq", "rst"}, {"uvw", "xyz"}};
    table::TablePtr table = createMSingleColumnTableByVec(columnName, expectedValue);
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ColumnData<autil::MultiString> *columnData
        = TableUtil::getColumnData<autil::MultiString>(table, columnName);
    ASSERT_TRUE(columnData != nullptr);

    vector<Offset<isearch::fbs::Column>> fbsColumnVec;
    SqlResultUtil(true).CreateSqlTableColumnByMultiByteString(
        columnName, columnData, fbb, fbsColumnVec);
    ASSERT_EQ(1, fbsColumnVec.size());
    auto fbsColumnOffset = fbsColumnVec[0];
    fbb.Finish(fbsColumnOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data);
    const flatbuffers::String *testColumnName = fbsColumn->name();
    ASSERT_TRUE(testColumnName != nullptr);
    ASSERT_EQ(columnName, testColumnName->str());
    ASSERT_NO_FATAL_FAILURE(checkMultiByteStringColumnMultiValue(fbsColumn, expectedValue));
    {
        TablePtr outputTable;
        auto asanPool = std::make_shared<autil::mem_pool::PoolAsan>();
        outputTable.reset(new table::Table(asanPool));
        outputTable->batchAllocateRow(table->getRowCount());
        auto *column = outputTable->declareColumn<autil::MultiString>("a");
        ASSERT_NE(nullptr, column);
        auto *columnData = column->getColumnData<autil::MultiString>();
        ASSERT_NE(nullptr, columnData);
        ASSERT_TRUE(SqlResultUtil().FromSqlTableColumnByMultiByteString(
            outputTable->getDataPool(), fbsColumn, columnData));
        ASSERT_NO_FATAL_FAILURE(
            TableTestUtil::checkOutputMultiColumn(outputTable, "a", expectedValue));
    }
}

TEST_F(SqlResultUtilTest, testCreateSqlTableSingleNumeric) {
    createSingleNumericTable();
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    Offset<isearch::fbs::TwoDimTable> fbsSqlTableOffset
        = SqlResultUtil().CreateSqlTable(_table, fbb);
    fbb.Finish(fbsSqlTableOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::TwoDimTable *fbsTable = GetRoot<isearch::fbs::TwoDimTable>(data);
    ASSERT_TRUE(fbsTable != nullptr);
    ASSERT_EQ(4u, fbsTable->rowCount());
    const Vector<Offset<isearch::fbs::Column>> *columns = fbsTable->columns();
    ASSERT_EQ(8u, columns->size());
#define CHECK_SINGLE_NUMERIC_COLUMN(columnType, columnName, columnIdx)                             \
    {                                                                                              \
        const isearch::fbs::Column *column = columns->Get(columnIdx);                              \
        ASSERT_TRUE(column != nullptr);                                                            \
        const flatbuffers::String *fieldName = column->name();                                     \
        ASSERT_EQ(string(columnName), fieldName->str());                                           \
        ASSERT_NO_FATAL_FAILURE(checkColumnValue<columnType>(column, {1, 2, 3, 4}));               \
    }
    CHECK_SINGLE_NUMERIC_COLUMN(uint8_t, "fuint8", 0);
    CHECK_SINGLE_NUMERIC_COLUMN(uint16_t, "fuint16", 1);
    CHECK_SINGLE_NUMERIC_COLUMN(uint32_t, "fuint32", 2);
    CHECK_SINGLE_NUMERIC_COLUMN(uint64_t, "fuint64", 3);
    CHECK_SINGLE_NUMERIC_COLUMN(int8_t, "fint8", 4);
    CHECK_SINGLE_NUMERIC_COLUMN(int16_t, "fint16", 5);
    CHECK_SINGLE_NUMERIC_COLUMN(int32_t, "fint32", 6);
    CHECK_SINGLE_NUMERIC_COLUMN(int64_t, "fint64", 7);
}

TEST_F(SqlResultUtilTest, testCreateSqlTableMultiNumeric) {
    createMultiNumericTable();
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    Offset<isearch::fbs::TwoDimTable> fbsSqlTableOffset
        = SqlResultUtil().CreateSqlTable(_table, fbb);
    fbb.Finish(fbsSqlTableOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::TwoDimTable *fbsTable = GetRoot<isearch::fbs::TwoDimTable>(data);
    ASSERT_TRUE(fbsTable != nullptr);
    ASSERT_EQ(4u, fbsTable->rowCount());
    const Vector<Offset<isearch::fbs::Column>> *columns = fbsTable->columns();
    ASSERT_EQ(8u, columns->size());
#define CHECK_MULTI_NUMERIC_COLUMN(columnType, columnName, columnIdx)                              \
    {                                                                                              \
        const isearch::fbs::Column *column = columns->Get(columnIdx);                              \
        ASSERT_TRUE(column != nullptr);                                                            \
        const flatbuffers::String *fieldName = column->name();                                     \
        ASSERT_EQ(string(columnName), fieldName->str());                                           \
        ASSERT_NO_FATAL_FAILURE(checkColumnMultiValue<columnType>(column, {{1}, {2}, {3}, {4}}));  \
    }

    CHECK_MULTI_NUMERIC_COLUMN(uint8_t, "fmuint8", 0);
    CHECK_MULTI_NUMERIC_COLUMN(uint16_t, "fmuint16", 1);
    CHECK_MULTI_NUMERIC_COLUMN(uint32_t, "fmuint32", 2);
    CHECK_MULTI_NUMERIC_COLUMN(uint64_t, "fmuint64", 3);
    CHECK_MULTI_NUMERIC_COLUMN(int8_t, "fmint8", 4);
    CHECK_MULTI_NUMERIC_COLUMN(int16_t, "fmint16", 5);
    CHECK_MULTI_NUMERIC_COLUMN(int32_t, "fmint32", 6);
    CHECK_MULTI_NUMERIC_COLUMN(int64_t, "fmint64", 7);
}

TEST_F(SqlResultUtilTest, testCreateSqlTableNoData) {
    createNoDataTable();
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    Offset<isearch::fbs::TwoDimTable> fbsSqlTableOffset
        = SqlResultUtil().CreateSqlTable(_table, fbb);
    fbb.Finish(fbsSqlTableOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::TwoDimTable *fbsTable = GetRoot<isearch::fbs::TwoDimTable>(data);
    ASSERT_TRUE(fbsTable != nullptr);
    ASSERT_EQ(0u, fbsTable->rowCount());
    const Vector<Offset<isearch::fbs::Column>> *columns = fbsTable->columns();
    ASSERT_EQ(8u, columns->size());
#define CHECK_NO_DATA_COLUMN(columnType, columnName, columnIdx)                                    \
    {                                                                                              \
        const isearch::fbs::Column *column = columns->Get(columnIdx);                              \
        ASSERT_TRUE(column != nullptr);                                                            \
        const flatbuffers::String *fieldName = column->name();                                     \
        ASSERT_EQ(string(columnName), fieldName->str());                                           \
        ASSERT_NO_FATAL_FAILURE(checkColumnValue<columnType>(column, {}));                         \
    }
    CHECK_NO_DATA_COLUMN(uint8_t, "fuint8", 0);
    CHECK_NO_DATA_COLUMN(uint16_t, "fuint16", 1);
    CHECK_NO_DATA_COLUMN(uint32_t, "fuint32", 2);
    CHECK_NO_DATA_COLUMN(uint64_t, "fuint64", 3);
    CHECK_NO_DATA_COLUMN(int8_t, "fint8", 4);
    CHECK_NO_DATA_COLUMN(int16_t, "fint16", 5);
    CHECK_NO_DATA_COLUMN(int32_t, "fint32", 6);
    CHECK_NO_DATA_COLUMN(int64_t, "fint64", 7);
}

TEST_F(SqlResultUtilTest, testCreateSqlErrorResult) {
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setErrorMsg("error message");
    Offset<isearch::fbs::SqlErrorResult> fbsSqlErrorResultOffset
        = SqlResultUtil().CreateSqlErrorResult(errorResult, fbb);
    fbb.Finish(fbsSqlErrorResultOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::SqlErrorResult *fbsErrorResult
        = GetRoot<isearch::fbs::SqlErrorResult>(data);
    ASSERT_TRUE(fbsErrorResult != nullptr);
    const flatbuffers::String *errorDesc = fbsErrorResult->errorDescription();
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, errorDesc->str());
    ASSERT_EQ(ERROR_UNKNOWN, fbsErrorResult->errorCode());
}

TEST_F(SqlResultUtilTest, testCreateAndFromSqlTable) {
    createStringTable();
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    Offset<isearch::fbs::TwoDimTable> fbsSqlTableOffset
        = SqlResultUtil().CreateSqlTable(_table, fbb);
    fbb.Finish(fbsSqlTableOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::TwoDimTable *fbsTable = GetRoot<isearch::fbs::TwoDimTable>(data);
    ASSERT_TRUE(fbsTable != nullptr);

    TablePtr table;
    {
        auto asanPool = std::make_shared<autil::mem_pool::PoolAsan>();
        table.reset(new table::Table(asanPool));
    }
    ASSERT_TRUE(SqlResultUtil().FromSqlTable(fbsTable, table));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn(
        table,
        "fmstring",
        {{"abc", "def", "g"}, {"hij", "klm", "n"}, {"opq", "rst"}, {"uvw", "xyz"}}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(
        table, "fstring", {"abcdefg", "hijklmn", "opqrst", "uvwxyz"}));
}

TEST_F(SqlResultUtilTest, testCreateFBSqlResult) {
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setErrorMsg("error message");
    QrsSessionSqlResult sqlResult;
    sqlResult.errorResult = errorResult;

    createStringTable();
    sqlResult.table = _table;

    double processTime = 1.0;
    uint32_t rowCount = 1000;
    double coveredPercent = 0.5;
    string searchInfo = "searchInfoTest";
    Offset<isearch::fbs::SqlResult> fbsSqlResultOffset
        = SqlResultUtil().CreateFBSqlResult(processTime,
                                            rowCount,
                                            coveredPercent,
                                            searchInfo,
                                            sqlResult,
                                            {{"table_a", true}},
                                            {{"table_b", 123}},
                                            true,
                                            fbb);
    fbb.Finish(fbsSqlResultOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    size_t dataLen = fbb.GetSize();
    flatbuffers::Verifier verifier((const uint8_t *)data, dataLen);
    ASSERT_TRUE(VerifySqlResultBuffer(verifier));

    const isearch::fbs::SqlResult *fbsSqlResult = GetRoot<isearch::fbs::SqlResult>(data);
    ASSERT_TRUE(fbsSqlResult != nullptr);
    ASSERT_DOUBLE_EQ(processTime, fbsSqlResult->processTime());
    ASSERT_EQ(rowCount, fbsSqlResult->rowCount());
    ASSERT_DOUBLE_EQ(coveredPercent, fbsSqlResult->coveredPercent());
    ASSERT_TRUE(fbsSqlResult->hasSoftFailure());
    ASSERT_EQ(searchInfo, fbsSqlResult->searchInfo()->str());
    const SqlErrorResult *fbsErrorResult = fbsSqlResult->errorResult();
    const flatbuffers::String *errorDesc = fbsErrorResult->errorDescription();
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, errorDesc->str());
    ASSERT_EQ(ERROR_UNKNOWN, fbsErrorResult->errorCode());
    const Vector<Offset<isearch::fbs::TableLeaderInfo>> *leaderInfos = fbsSqlResult->leaderInfos();
    ASSERT_EQ(1u, leaderInfos->size());
    {
        const isearch::fbs::TableLeaderInfo *info = leaderInfos->Get(0);
        ASSERT_TRUE(info != nullptr);
        const flatbuffers::String *fieldName = info->tableName();
        ASSERT_EQ(string("table_a"), fieldName->str());
        ASSERT_TRUE(info->isLeader());
    }
    const Vector<Offset<isearch::fbs::TableBuildWatermark>> *watermarks
        = fbsSqlResult->buildWatermarks();
    ASSERT_EQ(1u, watermarks->size());
    {
        const isearch::fbs::TableBuildWatermark *info = watermarks->Get(0);
        ASSERT_TRUE(info != nullptr);
        const flatbuffers::String *fieldName = info->tableName();
        ASSERT_EQ(string("table_b"), fieldName->str());
        ASSERT_EQ(123, info->buildWatermark());
    }

    const isearch::fbs::TwoDimTable *fbsTable = fbsSqlResult->sqlTable();
    ASSERT_TRUE(fbsTable != nullptr);
    const Vector<Offset<isearch::fbs::Column>> *columns = fbsTable->columns();
    ASSERT_EQ(2u, columns->size());
    {
        const isearch::fbs::Column *column = columns->Get(0);
        ASSERT_TRUE(column != nullptr);
        const flatbuffers::String *fieldName = column->name();
        ASSERT_EQ(string("fmstring"), fieldName->str());
        ASSERT_NO_FATAL_FAILURE(checkColumnMultiValue<string>(
            column, {{"abc", "def", "g"}, {"hij", "klm", "n"}, {"opq", "rst"}, {"uvw", "xyz"}}));
    }

    {
        const isearch::fbs::Column *column = columns->Get(1);
        ASSERT_TRUE(column != nullptr);
        const flatbuffers::String *fieldName = column->name();
        ASSERT_EQ(string("fstring"), fieldName->str());
        ASSERT_NO_FATAL_FAILURE(
            checkColumnValue<string>(column, {"abcdefg", "hijklmn", "opqrst", "uvwxyz"}));
    }
}

TEST_F(SqlResultUtilTest, testFromFBSqlResult) {
    FlatBufferSimpleAllocator flatbufferAllocator(_poolPtr.get());
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setErrorMsg("error message");
    QrsSessionSqlResult sqlResult;
    sqlResult.errorResult = errorResult;

    createStringTable();
    sqlResult.table = _table;

    double processTime = 1.0;
    uint32_t rowCount = 1000;
    double coveredPercent = 0.5;
    string searchInfo = "searchInfoTest";
    Offset<isearch::fbs::SqlResult> fbsSqlResultOffset
        = SqlResultUtil().CreateFBSqlResult(processTime,
                                            rowCount,
                                            coveredPercent,
                                            searchInfo,
                                            sqlResult,
                                            {{"table_a", true}},
                                            {{"table_b", 123}},
                                            true,
                                            fbb);
    fbb.Finish(fbsSqlResultOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    size_t dataLen = fbb.GetSize();
    flatbuffers::Verifier verifier((const uint8_t *)data, dataLen);
    ASSERT_TRUE(VerifySqlResultBuffer(verifier));

    const isearch::fbs::SqlResult *fbsSqlResult = GetRoot<isearch::fbs::SqlResult>(data);
    ASSERT_TRUE(fbsSqlResult != nullptr);
    ErrorResult outputErrorResult;
    bool hasSoftFailure = false;
    auto asanPool = std::make_shared<autil::mem_pool::PoolAsan>();
    auto outputTable = std::make_shared<table::Table>(asanPool);
    ASSERT_TRUE(SqlResultUtil().FromFBSqlResult(
        fbsSqlResult, outputErrorResult, hasSoftFailure, outputTable));
    ASSERT_EQ(errorResult.toJsonString(), outputErrorResult.toJsonString());
    ASSERT_TRUE(hasSoftFailure);
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputMultiColumn(
        outputTable,
        "fmstring",
        {{"abc", "def", "g"}, {"hij", "klm", "n"}, {"opq", "rst"}, {"uvw", "xyz"}}));
    ASSERT_NO_FATAL_FAILURE(TableTestUtil::checkOutputColumn(
        outputTable, "fstring", {"abcdefg", "hijklmn", "opqrst", "uvwxyz"}));
}

} // namespace sql
