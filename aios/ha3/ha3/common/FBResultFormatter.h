#ifndef ISEARCH_FBRESULTFORMATTER_H
#define ISEARCH_FBRESULTFORMATTER_H

#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <string>
#include <ha3/proto/SummaryResult_generated.h>
#include <ha3/common/Result.h>
#include <autil/mem_pool/Pool.h>
BEGIN_HA3_NAMESPACE(common);

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

class FBResultFormatter
{
public:
    const static uint32_t SUMMARY_IN_BYTES = 1;
    const static uint32_t PB_MATCHDOC_FORMAT = 2;
public:
    FBResultFormatter(autil::mem_pool::Pool *pool);
    ~FBResultFormatter();
public:
    std::string format(const ResultPtr &result);
private:
    SummaryHit *findFirstNonEmptySummaryHit(const std::vector<HitPtr> &hitVec);
private:
    flatbuffers::Offset<isearch::fbs::SummaryResult> CreateSummaryResult(
            const HA3_NS(common)::ResultPtr &resultPtr,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<flatbuffers::Vector<flatbuffers::Offset<isearch::fbs::FBErrorResult> > >
    CreateErrorResults(
            const HA3_NS(common)::MultiErrorResultPtr &multiErrorResult,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<flatbuffers::String> CreateTracer(
            const suez::turing::Tracer *tracer,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::TwoDimTable> CreateSummaryTable(
            const Hits *hits, flatbuffers::FlatBufferBuilder &fbb);

    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByString(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);

    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByUInt8(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByInt8(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByUInt16(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByInt16(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByUInt32(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByInt32(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByUInt64(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByInt64(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByFloat(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
    flatbuffers::Offset<isearch::fbs::Column> CreateSummaryTableColumnByDouble(
            const std::string &fieldName,
            size_t summaryFieldIdx,
            const std::vector<HA3_NS(common)::HitPtr> &hitVec,
            flatbuffers::FlatBufferBuilder &fbb);
private:
    autil::mem_pool::Pool *_pool;
private:
    friend class FBResultFormatterTest;

private:
    HA3_LOG_DECLARE();
};

END_HA3_NAMESPACE(common);


#endif //ISEARCH_FBRESULTFORMATTER_H
