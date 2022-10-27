#ifndef ISEARCH_SQLRESULTUTIL_H
#define ISEARCH_SQLRESULTUTIL_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <ha3/service/QrsSessionSqlResult.h>
#include <ha3/proto/SqlResult_generated.h>
#include <ha3/proto/TwoDimTable_generated.h>
#include <autil/mem_pool/Pool.h>
#include <autil/MultiValueType.h>
#include <string>

BEGIN_HA3_NAMESPACE(sql);

class FlatBufferSimpleAllocator : public flatbuffers::Allocator {
 public:
    explicit FlatBufferSimpleAllocator(autil::mem_pool::Pool *pool) : pool_(pool) {
    }
    uint8_t *allocate(size_t size) override {
        return reinterpret_cast<uint8_t *>(pool_->allocate(size));
    }
    void deallocate(uint8_t *p, size_t size) override{
    }

 private:
    autil::mem_pool::Pool *pool_;
};

class SqlResultUtil
{
public:
    SqlResultUtil();
    ~SqlResultUtil();
private:
    SqlResultUtil(const SqlResultUtil &);
    SqlResultUtil& operator=(const SqlResultUtil &);
public:
    static void toFlatBuffersString(
            double processTime,
            uint32_t rowCount,
            const std::string &searchInfo,
            HA3_NS(service)::QrsSessionSqlResult &sqlResult,
            autil::mem_pool::Pool *pool);

    static flatbuffers::Offset<isearch::fbs::SqlResult> CreateFBSqlResult(
            double processTime,
            uint32_t rowCount,
            const std::string &searchInfo,
            const HA3_NS(service)::QrsSessionSqlResult &sqlResult,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::SqlErrorResult> CreateSqlErrorResult(
            const HA3_NS(common)::ErrorResult &errorResult,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::TwoDimTable> CreateSqlTable(
            const TablePtr table, flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByString(
            const std::string &columnName,
            ColumnPtr column,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByMultiString(
            const std::string &columnName,
            ColumnData<autil::MultiString> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByUInt8(
            const std::string &columnName,
            ColumnData<uint8_t> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByInt8(
            const std::string &columnName,
            ColumnData<int8_t> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByUInt16(
            const std::string &columnName,
            ColumnData<uint16_t> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByInt16(
            const std::string &columnName,
            ColumnData<int16_t> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByUInt32(
            const std::string &columnName,
            ColumnData<uint32_t> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByInt32(
            const std::string &columnName,
            ColumnData<int32_t> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByUInt64(
            const std::string &columnName,
            ColumnData<uint64_t> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByInt64(
            const std::string &columnName,
            ColumnData<int64_t> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByFloat(
            const std::string &columnName,
            ColumnData<float> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);
    static flatbuffers::Offset<isearch::fbs::Column> CreateSqlTableColumnByDouble(
            const std::string &columnName,
            ColumnData<double> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiUInt8(
            const std::string &columnName,
            ColumnData<autil::MultiUInt8> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiInt8(
            const std::string &columnName,
            ColumnData<autil::MultiInt8> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiUInt16(
            const std::string &columnName,
            ColumnData<autil::MultiUInt16> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiInt16(
            const std::string &columnName,
            ColumnData<autil::MultiInt16> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiUInt32(
            const std::string &columnName,
            ColumnData<autil::MultiUInt32> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiInt32(
            const std::string &columnName,
            ColumnData<autil::MultiInt32> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiUInt64(
            const std::string &columnName,
            ColumnData<autil::MultiUInt64> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiInt64(
            const std::string &columnName,
            ColumnData<autil::MultiInt64> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiFloat(
            const std::string &columnName,
            ColumnData<autil::MultiFloat> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

    static flatbuffers::Offset<isearch::fbs::Column>
    CreateSqlTableColumnByMultiDouble(
            const std::string &columnName,
            ColumnData<autil::MultiDouble> *columnData,
            TablePtr table,
            flatbuffers::FlatBufferBuilder &fbb);

private:
    HA3_LOG_DECLARE();

};

HA3_TYPEDEF_PTR(SqlResultUtil);

END_HA3_NAMESPACE(sql);

#endif //ISEARCH_SQLRESULTUTIL_H
