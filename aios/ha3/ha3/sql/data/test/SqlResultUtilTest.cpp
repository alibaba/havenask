#include <unittest/unittest.h>
#include <ha3/test/test.h>
#include <ha3/sql/data/SqlResultUtil.h>
#include <ha3/sql/data/TableUtil.h>
#include <ha3/sql/ops/test/OpTestBase.h>
#include <ha3/common/ErrorDefine.h>
#include <ha3/common/ErrorResult.h>
#include <autil/ConstString.h>

using namespace std;
using namespace testing;
using namespace matchdoc;
using namespace autil;
using namespace flatbuffers;
using namespace isearch::fbs;
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(service);
BEGIN_HA3_NAMESPACE(sql);

class SqlResultUtilTest : public OpTestBase {
public:
    void setUp();
    void tearDown();
public:
    void createSingleNumericTable() {
        const auto &docs = getMatchDocs(_allocator, 4);
        extendMatchDocAllocator<uint8_t>(_allocator, docs, "fuint8", {1, 2, 3, 4});
        extendMatchDocAllocator<uint16_t>(_allocator, docs, "fuint16", {1, 2, 3, 4});
        extendMatchDocAllocator<uint32_t>(_allocator, docs, "fuint32", {1, 2, 3, 4});
        extendMatchDocAllocator<uint64_t>(_allocator, docs, "fuint64", {1, 2, 3, 4});
        extendMatchDocAllocator<int8_t>(_allocator, docs, "fint8", {1, 2, 3, 4});
        extendMatchDocAllocator<int16_t>(_allocator, docs, "fint16", {1, 2, 3, 4});
        extendMatchDocAllocator<int32_t>(_allocator, docs, "fint32", {1, 2, 3, 4});
        extendMatchDocAllocator<int64_t>(_allocator, docs, "fint64", {1, 2, 3, 4});
        _table.reset(new Table(docs, _allocator));
    }

    void createMultiNumericTable() {
        const auto &docs = getMatchDocs(_allocator, 4);
        extendMultiValueMatchDocAllocator<uint8_t>(_allocator, docs, "fmuint8", {
                    {1}, {2}, {3}, {4}});
        extendMultiValueMatchDocAllocator<uint16_t>(_allocator, docs, "fmuint16", {
                    {1}, {2}, {3}, {4}});
        extendMultiValueMatchDocAllocator<uint32_t>(_allocator, docs, "fmuint32", {
                    {1}, {2}, {3}, {4}});
        extendMultiValueMatchDocAllocator<uint64_t>(_allocator, docs, "fmuint64", {
                    {1}, {2}, {3}, {4}});
        extendMultiValueMatchDocAllocator<int8_t>(_allocator, docs, "fmint8", {
                    {1}, {2}, {3}, {4}});
        extendMultiValueMatchDocAllocator<int16_t>(_allocator, docs, "fmint16", {
                    {1}, {2}, {3}, {4}});
        extendMultiValueMatchDocAllocator<int32_t>(_allocator, docs, "fmint32", {
                    {1}, {2}, {3}, {4}});
        extendMultiValueMatchDocAllocator<int64_t>(_allocator, docs, "fmint64", {
                    {1}, {2}, {3}, {4}});

        _table.reset(new Table(docs, _allocator));
    }


    void createStringTable() {
        const auto &docs = getMatchDocs(_allocator, 4);
        extendMultiValueMatchDocAllocator<string>(_allocator, docs, "fmstring",
            {
                {"abc", "def", "g"}, {"hij", "klm", "n"},
                                     {"opq", "rst"}, {"uvw", "xyz"}});
        extendMatchDocAllocator(_allocator, docs, "fstring", {
                    "abcdefg", "hijklmn", "opqrst", "uvwxyz"});
        _table.reset(new Table(docs, _allocator));
    }

    template<typename T>
    TablePtr createSingleColumnTableByVec(const string &columnName,
            const vector<T> &values) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, values.size());
        extendMatchDocAllocator<T>(allocator, docs, columnName, values);
        return TablePtr(new Table(docs, allocator));
    }

    TablePtr createSingleStringColumnTableByVec(const string &columnName,
            const vector<string> &values) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, values.size());
        extendMatchDocAllocator(allocator, docs, columnName, values);
        return TablePtr(new Table(docs, allocator));
    }

    template<typename T>
    TablePtr createMSingleColumnTableByVec(const string &columnName,
            const vector<vector<T> > &values) {
        MatchDocAllocatorPtr allocator;
        const auto &docs = getMatchDocs(allocator, values.size());
        extendMultiValueMatchDocAllocator<T>(allocator, docs, columnName, values);
        return TablePtr(new Table(docs, allocator));
    }


public:
    TablePtr _table;
    MatchDocAllocatorPtr _allocator;
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(table, SqlResultUtilTest);

void SqlResultUtilTest::setUp() {
}

void SqlResultUtilTest::tearDown() {
}

#define DECLARE_SINGLE_NUMERIC_TYPE_PARTIAL_TEMPLATE(fieldType, columnType) \
    template<>                                                          \
    void checkColumnValue<fieldType>(const isearch::fbs::Column *fbsColumn,\
            const std::vector<fieldType> &expectedValues) {             \
        ASSERT_EQ(ColumnType_##columnType##Column, fbsColumn->value_type()); \
        const auto *column =                                            \
                              fbsColumn->value_as_##columnType##Column(); \
        ASSERT_TRUE(column != nullptr);                                 \
        const auto *columnValue = column->value();                      \
        ASSERT_EQ(expectedValues.size(), columnValue->size()); \
        for (size_t i=0; i<expectedValues.size(); i++) {       \
            ASSERT_EQ(expectedValues[i], columnValue->Get(i));          \
        }                                                               \
    }

template <typename T>
void checkColumnValue(const isearch::fbs::Column *fbsColumn,
                      const std::vector<T> &expectedValues) = delete;
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

template<>
void checkColumnValue<string>(const isearch::fbs::Column *fbsColumn,
                              const std::vector<string> &expectedValues) {
    ASSERT_EQ(ColumnType_StringColumn, fbsColumn->value_type());
    const auto *column = fbsColumn->value_as_StringColumn();
    ASSERT_TRUE(column != nullptr);
    const auto *columnValue = column->value();
    ASSERT_EQ(expectedValues.size(), columnValue->size());
    for (size_t i=0; i<expectedValues.size(); i++) {
        ASSERT_EQ(expectedValues[i], columnValue->Get(i)->str());
    }
}

#define DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_SINGLE_NUMERIC_TYPE(fbType, fieldType) \
    TEST_F(SqlResultUtilTest, testCreateSqlTableColumnBy##fbType) {     \
       string columnName = "f"#fieldType;                               \
       fieldType maxValue = std::numeric_limits<fieldType>::max();      \
       vector<fieldType> expectedValue = {maxValue,                     \
                                          --maxValue,                   \
                                          --maxValue,                   \
                                          --maxValue};                  \
       TablePtr table = createSingleColumnTableByVec(columnName, expectedValue); \
       FlatBufferSimpleAllocator flatbufferAllocator(&_pool);           \
       FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);          \
       ColumnData<fieldType> *column = TableUtil::getColumnData<fieldType>( \
               table, columnName);                                      \
       ASSERT_TRUE(column != nullptr);                                  \
       Offset<isearch::fbs::Column> fbsColumnOffset =                   \
                         SqlResultUtil::CreateSqlTableColumnBy##fbType( \
                                    columnName, column, table, fbb);    \
        fbb.Finish(fbsColumnOffset);                                        \
        char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());  \
        const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data); \
        const flatbuffers::String *testColumnName = fbsColumn->name();  \
        ASSERT_TRUE(testColumnName != nullptr);                         \
        ASSERT_EQ(columnName, testColumnName->str());                   \
        checkColumnValue<fieldType>(fbsColumn, expectedValue);          \
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
                           const std::vector<vector<T> > &expectedValues) = delete;

#define DECLARE_MULTI_NUMERIC_TYPE_PARTIAL_TEMPLATE(fieldType, columnType) \
    template<>                                                          \
    void checkColumnMultiValue(const isearch::fbs::Column *fbsColumn,   \
                               const vector<vector<fieldType> > &expectedValues) { \
        ASSERT_EQ(ColumnType_Multi##columnType##Column, fbsColumn->value_type()); \
        const auto *column =                                            \
                       fbsColumn->value_as_Multi##columnType##Column(); \
        ASSERT_TRUE(column != nullptr);                                 \
        const auto *columnValue = column->value();                      \
        ASSERT_EQ(expectedValues.size(), columnValue->size());          \
        for (size_t i = 0; i<expectedValues.size(); i++) {              \
            const Vector<fieldType> *valueVec = columnValue->Get(i)->value(); \
            ASSERT_TRUE(valueVec != nullptr);                           \
            ASSERT_EQ(expectedValues[i].size(), valueVec->size());      \
            for (size_t j = 0; j < expectedValues[i].size(); j++) {     \
                ASSERT_EQ(expectedValues[i][j], valueVec->Get(j));      \
            }                                                           \
        }                                                               \
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

template<>
void checkColumnMultiValue(const isearch::fbs::Column *fbsColumn,
                           const vector<vector<string> > &expectedValues) {
    ASSERT_EQ(ColumnType_MultiStringColumn, fbsColumn->value_type());
    const auto *column =
        fbsColumn->value_as_MultiStringColumn();
    ASSERT_TRUE(column != nullptr);
    const auto *columnValue = column->value();
    ASSERT_EQ(expectedValues.size(), columnValue->size());
    for (size_t i = 0; i<expectedValues.size(); i++) {
        const Vector<flatbuffers::Offset<flatbuffers::String> > *valueVec =
            columnValue->Get(i)->value();
        ASSERT_TRUE(valueVec != nullptr);
        ASSERT_EQ(expectedValues[i].size(), valueVec->size());
        for (size_t j = 0; j < expectedValues[i].size(); j++) {
            ASSERT_EQ(expectedValues[i][j], valueVec->Get(j)->str());
        }
    }
}

#define DECLARE_TEST_CREATE_SQL_TABLE_COLUMN_BY_MULTI_NUMERIC_TYPE(fbType, mfieldType) \
    TEST_F(SqlResultUtilTest, testCreateSqlTableColumnByMulti##fbType) { \
       string columnName = "fm"#mfieldType;                             \
       typedef mfieldType::type fieldType;                              \
       fieldType maxValue = std::numeric_limits<fieldType>::max();      \
       vector<vector<fieldType> > expectedValue = { {0, 1, 2},          \
                                                    {3, 4, 5},          \
                                                    {maxValue,          \
                                                     --maxValue,        \
                                                     --maxValue},       \
                                                    {--maxValue,        \
                                                     --maxValue,        \
                                                     --maxValue}};      \
       TablePtr table = createMSingleColumnTableByVec(columnName, expectedValue); \
       FlatBufferSimpleAllocator flatbufferAllocator(&_pool);           \
       FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);          \
       ColumnData<mfieldType> *column = TableUtil::getColumnData<mfieldType>( \
               table, columnName);                                      \
       ASSERT_TRUE(column != nullptr);                                  \
       Offset<isearch::fbs::Column> fbsColumnOffset =                   \
                         SqlResultUtil::CreateSqlTableColumnByMulti##fbType( \
                                    columnName, column, table, fbb);    \
        fbb.Finish(fbsColumnOffset);                                        \
        char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());  \
        const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data); \
        const flatbuffers::String *testColumnName = fbsColumn->name();  \
        ASSERT_TRUE(testColumnName != nullptr);                         \
        ASSERT_EQ(columnName, testColumnName->str());                   \
        checkColumnMultiValue<fieldType>(fbsColumn, expectedValue);          \
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

TEST_F(SqlResultUtilTest, testCreateSqlTableColumnByString) {
    string columnName = "fstring";
    vector<string> expectedValue = {"abcdefg", "hijklmn",
                                    "opqrst", "uvwxyz"};
    TablePtr table = createSingleStringColumnTableByVec(columnName, expectedValue);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ColumnPtr column = table->getColumn(columnName);
    ASSERT_TRUE(column != nullptr);
    Offset<isearch::fbs::Column> fbsColumnOffset =
        SqlResultUtil::CreateSqlTableColumnByString(
                columnName, column, table, fbb);
    fbb.Finish(fbsColumnOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data);
    const flatbuffers::String *testColumnName = fbsColumn->name();
    ASSERT_TRUE(testColumnName != nullptr);
    ASSERT_EQ(columnName, testColumnName->str());
    checkColumnValue<string>(fbsColumn, expectedValue);
}

TEST_F(SqlResultUtilTest, testCreateSqlTableColumnByMultiString) {
    string columnName = "fmstring";
    vector<vector<string> > expectedValue = {{"abc", "def", "g"}, {"hij", "klm", "n"},
                                    {"opq", "rst"}, {"uvw", "xyz"}};
    TablePtr table = createMSingleColumnTableByVec(columnName, expectedValue);
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ColumnData<autil::MultiString> *columnData =
        TableUtil::getColumnData<autil::MultiString>(table, columnName);
    ASSERT_TRUE(columnData != nullptr);
    Offset<isearch::fbs::Column> fbsColumnOffset =
        SqlResultUtil::CreateSqlTableColumnByMultiString(
                columnName, columnData, table, fbb);
    fbb.Finish(fbsColumnOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::Column *fbsColumn = GetRoot<isearch::fbs::Column>(data);
    const flatbuffers::String *testColumnName = fbsColumn->name();
    ASSERT_TRUE(testColumnName != nullptr);
    ASSERT_EQ(columnName, testColumnName->str());
    checkColumnMultiValue<string>(fbsColumn, expectedValue);
}

TEST_F(SqlResultUtilTest, testCreateSqlTableSingleNumeric) {
    createSingleNumericTable();
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    Offset<isearch::fbs::TwoDimTable> fbsSqlTableOffset =
        SqlResultUtil::CreateSqlTable(_table, fbb);
    fbb.Finish(fbsSqlTableOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::TwoDimTable *fbsTable =
        GetRoot<isearch::fbs::TwoDimTable>(data);
    ASSERT_TRUE(fbsTable != nullptr);
    const Vector<Offset<isearch::fbs::Column> > *columns = fbsTable->columns();
    ASSERT_EQ(8u, columns->size());
#define CHECK_SINGLE_NUMERIC_COLUMN(columnType, columnName, columnIdx)  \
    {                                                                   \
        const isearch::fbs::Column *column = columns->Get(columnIdx);  \
        ASSERT_TRUE(column != nullptr);                                \
        const flatbuffers::String *fieldName = column->name();         \
        ASSERT_EQ(string(columnName), fieldName->str());               \
        checkColumnValue<columnType>(column, {1, 2, 3, 4});           \
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
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    Offset<isearch::fbs::TwoDimTable> fbsSqlTableOffset =
        SqlResultUtil::CreateSqlTable(_table, fbb);
    fbb.Finish(fbsSqlTableOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::TwoDimTable *fbsTable =
        GetRoot<isearch::fbs::TwoDimTable>(data);
    ASSERT_TRUE(fbsTable != nullptr);
    const Vector<Offset<isearch::fbs::Column> > *columns = fbsTable->columns();
    ASSERT_EQ(8u, columns->size());
#define CHECK_MULTI_NUMERIC_COLUMN(columnType, columnName, columnIdx)  \
    {                                                                   \
        const isearch::fbs::Column *column = columns->Get(columnIdx);  \
        ASSERT_TRUE(column != nullptr);                                \
        const flatbuffers::String *fieldName = column->name();         \
        ASSERT_EQ(string(columnName), fieldName->str());               \
        checkColumnMultiValue<columnType>(column, {{1}, {2}, {3}, {4}}); \
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

TEST_F(SqlResultUtilTest, testCreateSqlTableString) {
    createStringTable();
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    Offset<isearch::fbs::TwoDimTable> fbsSqlTableOffset =
        SqlResultUtil::CreateSqlTable(_table, fbb);
    fbb.Finish(fbsSqlTableOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::TwoDimTable *fbsTable =
        GetRoot<isearch::fbs::TwoDimTable>(data);
    ASSERT_TRUE(fbsTable != nullptr);
    const Vector<Offset<isearch::fbs::Column> > *columns = fbsTable->columns();
    ASSERT_EQ(2u, columns->size());
    {
        const isearch::fbs::Column *column = columns->Get(0);
        ASSERT_TRUE(column != nullptr);
        const flatbuffers::String *fieldName = column->name();
        ASSERT_EQ(string("fmstring"), fieldName->str());
        checkColumnMultiValue<string>(column, {{"abc", "def", "g"},
                    {"hij", "klm", "n"}, {"opq", "rst"}, {"uvw", "xyz"}});
    }

    {
        const isearch::fbs::Column *column = columns->Get(1);
        ASSERT_TRUE(column != nullptr);
        const flatbuffers::String *fieldName = column->name();
        ASSERT_EQ(string("fstring"), fieldName->str());
        checkColumnValue<string>(column, {"abcdefg", "hijklmn", "opqrst", "uvwxyz"});
    }

}


TEST_F(SqlResultUtilTest, testCreateSqlErrorResult) {
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");
    Offset<isearch::fbs::SqlErrorResult> fbsSqlErrorResultOffset =
        SqlResultUtil::CreateSqlErrorResult(errorResult, fbb);
    fbb.Finish(fbsSqlErrorResultOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    const isearch::fbs::SqlErrorResult *fbsErrorResult =
        GetRoot<isearch::fbs::SqlErrorResult>(data);
    ASSERT_TRUE(fbsErrorResult != nullptr);
    const flatbuffers::String * errorDesc = fbsErrorResult->errorDescription();
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, errorDesc->str());
    ASSERT_EQ(ERROR_UNKNOWN, fbsErrorResult->errorCode());
}

TEST_F(SqlResultUtilTest, testCreateFBSqlResult) {
    FlatBufferSimpleAllocator flatbufferAllocator(&_pool);
    FlatBufferBuilder fbb(40 * 1024, &flatbufferAllocator);
    ErrorResult errorResult(ERROR_UNKNOWN);
    errorResult.setHostInfo("12345", "test");
    errorResult.setErrorMsg("error message");
    QrsSessionSqlResult sqlResult;
    sqlResult.errorResult = errorResult;

    createStringTable();
    sqlResult.table = _table;

    double processTime = 1.0;
    uint32_t rowCount = 1000;
    string searchInfo = "searchInfoTest";
    Offset<isearch::fbs::SqlResult> fbsSqlResultOffset =
        SqlResultUtil::CreateFBSqlResult(processTime, rowCount, searchInfo,
                sqlResult, fbb);
    fbb.Finish(fbsSqlResultOffset);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    size_t dataLen = fbb.GetSize();
    flatbuffers::Verifier verifier((const uint8_t*)data, dataLen);
    ASSERT_TRUE(VerifySqlResultBuffer(verifier));

    const isearch::fbs::SqlResult *fbsSqlResult =
        GetRoot<isearch::fbs::SqlResult>(data);
    ASSERT_TRUE(fbsSqlResult != nullptr);
    ASSERT_DOUBLE_EQ(processTime, fbsSqlResult->processTime());
    ASSERT_DOUBLE_EQ(rowCount, fbsSqlResult->rowCount());
    ASSERT_EQ(searchInfo, fbsSqlResult->searchInfo()->str());
    const SqlErrorResult *fbsErrorResult = fbsSqlResult->errorResult();
    const flatbuffers::String * errorDesc = fbsErrorResult->errorDescription();
    string expectErrorMsg = haErrorCodeToString(ERROR_UNKNOWN) + " error message";
    ASSERT_EQ(expectErrorMsg, errorDesc->str());
    ASSERT_EQ(ERROR_UNKNOWN, fbsErrorResult->errorCode());

    const isearch::fbs::TwoDimTable *fbsTable = fbsSqlResult->sqlTable();
    ASSERT_TRUE(fbsTable != nullptr);
    const Vector<Offset<isearch::fbs::Column> > *columns = fbsTable->columns();
    ASSERT_EQ(2u, columns->size());
    {
        const isearch::fbs::Column *column = columns->Get(0);
        ASSERT_TRUE(column != nullptr);
        const flatbuffers::String *fieldName = column->name();
        ASSERT_EQ(string("fmstring"), fieldName->str());
        checkColumnMultiValue<string>(column, {{"abc", "def", "g"},
                    {"hij", "klm", "n"}, {"opq", "rst"}, {"uvw", "xyz"}});
    }

    {
        const isearch::fbs::Column *column = columns->Get(1);
        ASSERT_TRUE(column != nullptr);
        const flatbuffers::String *fieldName = column->name();
        ASSERT_EQ(string("fstring"), fieldName->str());
        checkColumnValue<string>(column, {"abcdefg", "hijklmn", "opqrst", "uvwxyz"});
    }
}


END_HA3_NAMESPACE(sql);
