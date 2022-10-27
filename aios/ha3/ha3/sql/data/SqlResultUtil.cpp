#include <ha3/sql/data/SqlResultUtil.h>
#include <autil/TimeUtility.h>
#include <matchdoc/ValueType.h>
#include <ha3/proto/SqlResult_generated.h>
#include <ha3/common/ErrorResult.h>

using namespace std;
using namespace autil;
using namespace matchdoc;
using namespace isearch::fbs;
using namespace autil::mem_pool;
using namespace flatbuffers;

USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(sql);
HA3_LOG_SETUP(sql, SqlResultUtil);
static const uint32_t FB_BUILDER_INIT_SIZE = 1024 * 1024;
SqlResultUtil::SqlResultUtil() {
}

SqlResultUtil::~SqlResultUtil() {
}

void SqlResultUtil::toFlatBuffersString(
        double processTime,
        uint32_t rowCount,
        const string &searchInfo,
        QrsSessionSqlResult &sqlResult,
        Pool *pool) {
    FlatBufferSimpleAllocator flatbufferAllocator(pool);
    FlatBufferBuilder fbb(FB_BUILDER_INIT_SIZE, &flatbufferAllocator);

    Offset<SqlResult> fbSqlResult = CreateFBSqlResult(processTime, rowCount,
            searchInfo, sqlResult, fbb);
    fbb.Finish(fbSqlResult);
    char *data = reinterpret_cast<char *>(fbb.GetBufferPointer());
    sqlResult.resultStr.assign(data, fbb.GetSize());
}

Offset<SqlResult> SqlResultUtil::CreateFBSqlResult(
        double processTime,
        uint32_t rowCount,
        const string &searchInfo,
        const QrsSessionSqlResult &sqlResult,
        FlatBufferBuilder &fbb) {
    return isearch::fbs::CreateSqlResult(fbb, processTime, rowCount,
            CreateSqlErrorResult(sqlResult.errorResult, fbb),
            CreateSqlTable(sqlResult.table, fbb),
            fbb.CreateString(searchInfo));
}

Offset<SqlErrorResult> SqlResultUtil::CreateSqlErrorResult(
        const ErrorResult &errorResult,
        FlatBufferBuilder &fbb) {
    return isearch::fbs::CreateSqlErrorResult(fbb,
            fbb.CreateString(errorResult.getPartitionID()),
            fbb.CreateString(errorResult.getHostName()),
            errorResult.getErrorCode(),
            fbb.CreateString(errorResult.getErrorDescription())
                                            );
}

#define NUMERIC_SWITCH_CASE_HELPER(MY_MACRO)       \
    MY_MACRO(UInt8, bt_uint8);                        \
    MY_MACRO(Int8, bt_int8);                          \
    MY_MACRO(UInt16, bt_uint16);                      \
    MY_MACRO(Int16, bt_int16);                        \
    MY_MACRO(UInt32, bt_uint32);                      \
    MY_MACRO(Int32, bt_int32);                        \
    MY_MACRO(UInt64, bt_uint64);                        \
    MY_MACRO(Int64, bt_int64);                          \
    MY_MACRO(Float, bt_float);                          \
    MY_MACRO(Double, bt_double);

#define NUMERIC_SWITCH_CASE(fbType, fieldType)                      \
    case fieldType:                                                     \
    {                                                                   \
        typedef MatchDocBuiltinType2CppType<fieldType, false>::CppType cppType; \
        ColumnData<cppType> *columnData =                               \
                                       table->getColumn(columnIdx) \
                                            ->getColumnData<cppType>(); \
        if (!columnData) {                                              \
            break;                                                      \
        }                                                               \
        sqlTableColumns.push_back(                                      \
                CreateSqlTableColumnBy##fbType(                         \
                        columnName, columnData, table, fbb));           \
        break;                                                          \
    }

#define MULTINUMERIC_SWITCH_CASE(fbType, fieldType)                      \
    case fieldType:                                                     \
    {                                                                   \
        typedef MatchDocBuiltinType2CppType<fieldType, true>::CppType cppType; \
        ColumnData<cppType> *columnData =                               \
                                       table->getColumn(columnIdx) \
                                            ->getColumnData<cppType>(); \
        if (!columnData) {                                              \
            break;                                                      \
        }                                                               \
        sqlTableColumns.push_back(                                      \
                CreateSqlTableColumnByMulti##fbType(                    \
                        columnName, columnData, table, fbb));           \
        break;                                                          \
    }

Offset<TwoDimTable> SqlResultUtil::CreateSqlTable(
        const TablePtr table,
        FlatBufferBuilder &fbb) {
    vector<Offset<isearch::fbs::Column> > sqlTableColumns;
    size_t rowCount = 0u;
    if (table) {
        rowCount = table->getRowCount();
        if (rowCount > 0) {
            size_t columnCount = table->getColumnCount();
            for (size_t columnIdx = 0; columnIdx < columnCount; columnIdx++) {
                const string &columnName = table->getColumnName(columnIdx);
                matchdoc::ValueType columnValueType = table->getColumnType(columnIdx);
                matchdoc::BuiltinType columnType = columnValueType.getBuiltinType();
                bool isMultiValue = columnValueType.isMultiValue();
                if (!isMultiValue) {
                    switch(columnType) {
                        NUMERIC_SWITCH_CASE_HELPER(NUMERIC_SWITCH_CASE);
                    default:
                        ColumnPtr column = table->getColumn(columnIdx);
                        sqlTableColumns.push_back(
                                CreateSqlTableColumnByString(
                                        columnName, column, table, fbb));
                    }
                }
                else {
                    switch(columnType) {
                        NUMERIC_SWITCH_CASE_HELPER(MULTINUMERIC_SWITCH_CASE);
                        case bt_string: {
                            ColumnData<autil::MultiString> *columnData =
                                table->getColumn(columnIdx)
                                ->getColumnData<autil::MultiString>();
                            sqlTableColumns.push_back(
                                    CreateSqlTableColumnByMultiString(
                                            columnName, columnData, table, fbb));
                            break;
                        }
                        default:
                        {
                            ColumnPtr column = table->getColumn(columnIdx);
                            sqlTableColumns.push_back(
                                    CreateSqlTableColumnByString(
                                            columnName, column, table, fbb));
                        }
                    }
                }
            }
        }
    }
#undef NUMERIC_SWITCH_CASE
#undef NUMERIC_SWITCH_CASE_HELPER

    return CreateTwoDimTable(
            fbb, (uint32_t) rowCount, fbb.CreateVector(sqlTableColumns));
}

#define DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(fbType, fieldType)      \
    Offset<isearch::fbs::Column> SqlResultUtil::CreateSqlTableColumnBy##fbType( \
            const string &columnName, ColumnData<fieldType> *columnData, \
            TablePtr table, FlatBufferBuilder &fbb) {                   \
        size_t rowCount = table->getRowCount();                         \
        vector<fieldType> columnValue(rowCount, 0);                     \
        for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx ++) {         \
            columnValue[rowIdx] = columnData->get(rowIdx);              \
        }                                                               \
        Offset<fbType##Column> fb_columnValue =                         \
                        Create##fbType##Column(fbb, fbb.CreateVector(columnValue)); \
        return CreateColumnDirect(fbb, columnName.c_str(), \
                                  ColumnType_##fbType##Column, fb_columnValue.Union()); \
    }




#define DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(fbType, fieldType) \
    Offset<isearch::fbs::Column> SqlResultUtil::CreateSqlTableColumnByMulti##fbType( \
            const string &columnName, ColumnData<fieldType> *columnData, \
            TablePtr table, FlatBufferBuilder &fbb) {                   \
        vector<Offset<isearch::fbs::Multi##fbType> > columnValue;       \
        size_t rowCount = table->getRowCount();                         \
        for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx ++) {         \
            fieldType *value = columnData->getPointer(rowIdx);          \
            columnValue.push_back(                                      \
                    CreateMulti##fbType(fbb,                            \
                            fbb.CreateVector<fieldType::type>(    \
                                    value->data(), value->size())));    \
        }                                                               \
        Offset<Multi##fbType##Column> fb_columnValue =                  \
                  CreateMulti##fbType##Column(fbb, fbb.CreateVector(columnValue)); \
        return CreateColumnDirect(fbb, columnName.c_str(), \
                ColumnType_Multi##fbType##Column, fb_columnValue.Union()); \
    }

Offset<isearch::fbs::Column> SqlResultUtil::CreateSqlTableColumnByString(
        const string &columnName, ColumnPtr column,
        TablePtr table, FlatBufferBuilder &fbb) {
    vector<Offset<flatbuffers::String> > columnValue;
    size_t rowCount = table->getRowCount();
    for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx ++) {
        string valueStr = column->toString(rowIdx);
        columnValue.push_back(
                fbb.CreateString(valueStr));
    }
    Offset<StringColumn> fb_columnValue =
        CreateStringColumn(fbb, fbb.CreateVector(columnValue));
    return CreateColumnDirect(fbb, columnName.c_str(),
                              ColumnType_StringColumn, fb_columnValue.Union());
}

Offset<isearch::fbs::Column> SqlResultUtil::CreateSqlTableColumnByMultiString(
        const string &columnName, ColumnData<autil::MultiString> *columnData,
        TablePtr table, FlatBufferBuilder &fbb) {
    vector<Offset<isearch::fbs::MultiString> > columnValue;
    size_t rowCount = table->getRowCount();
    for (size_t rowIdx = 0; rowIdx < rowCount; rowIdx ++) {
        autil::MultiString *value = columnData->getPointer(rowIdx);
        vector<string> strVec;
        for (size_t i = 0; i < value->size(); i ++) {
            MultiChar tmpStr = (*value)[i];
            strVec.push_back(string(tmpStr.data(), tmpStr.size()));
        }
        columnValue.push_back(
                CreateMultiString(fbb,
                        fbb.CreateVectorOfStrings(strVec)));
    }
    Offset<MultiStringColumn> fb_columnValue =
        CreateMultiStringColumn(fbb, fbb.CreateVector(columnValue));
    return CreateColumnDirect(fbb, columnName.c_str(),
                              ColumnType_MultiStringColumn, fb_columnValue.Union());
}

DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(UInt8, uint8_t);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(Int8, int8_t);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(UInt16, uint16_t);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(Int16, int16_t);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(UInt32, uint32_t);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(Int32, int32_t);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(UInt64, uint64_t);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(Int64, int64_t);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(Float, float);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE(Double, double);

DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(UInt8, autil::MultiUInt8);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(Int8, autil::MultiInt8);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(UInt16, autil::MultiUInt16);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(Int16, autil::MultiInt16);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(UInt32, autil::MultiUInt32);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(Int32, autil::MultiInt32);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(UInt64, autil::MultiUInt64);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(Int64, autil::MultiInt64);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(Float, autil::MultiFloat);
DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE(Double, autil::MultiDouble);

#undef DECLARE_CREATE_SQL_TABLE_COLUMN_BY_TYPE
#undef DECLARE_CREATE_SQL_TABLE_COLUMN_BY_MULTITYPE

END_HA3_NAMESPACE(sql);
