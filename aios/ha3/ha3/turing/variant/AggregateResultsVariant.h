#ifndef ISEARCH_HA3AGGRESULTSVARIANT_H
#define ISEARCH_HA3AGGRESULTSVARIANT_H

#include <autil/Log.h>
#include <tensorflow/core/framework/variant.h>
#include <tensorflow/core/framework/tensor.h>
#include <tensorflow/core/framework/variant_encode_decode.h>
#include <ha3/common/AggregateResult.h>


BEGIN_HA3_NAMESPACE(turing);
class AggregateResultsVariant
{
public:
    AggregateResultsVariant();
    AggregateResultsVariant(HA3_NS(common)::AggregateResultsPtr aggResults,
                            autil::mem_pool::Pool *pool);
    AggregateResultsVariant(HA3_NS(common)::AggregateResultsPtr aggResults,
                            uint64_t toAggCount,
                            uint64_t aggCount,
                            autil::mem_pool::Pool *pool);
    AggregateResultsVariant(const AggregateResultsVariant &other);
    ~AggregateResultsVariant();
public:
    void Encode(tensorflow::VariantTensorData* data) const;
    bool Decode(const tensorflow::VariantTensorData& data);
    std::string TypeName() const {
        return "Ha3AggregateResults";
    }
    HA3_NS(common)::AggregateResultsPtr getAggResults() const { return _aggResults; }
    uint64_t getAggCount() const { return _aggCount; }
    uint64_t getToAggCount() const { return _toAggCount; }
    uint64_t getSeekedCount() const { return _seekedCount; }
    uint64_t getSeekTermDocCount() const { return _seekTermDocCount; }

    void setSeekedCount(uint64_t seekedCount) {
        _seekedCount = seekedCount;
    }
    void setSeekTermDocCount(uint64_t seekTermDocCount) {
        _seekTermDocCount = seekTermDocCount;
    }
public:
    bool construct(autil::mem_pool::Pool *outPool);
private:
    HA3_NS(common)::AggregateResultsPtr _aggResults;
    uint64_t _toAggCount;
    uint64_t _aggCount;
    uint64_t _seekedCount;
    uint64_t _seekTermDocCount;
    autil::mem_pool::Pool* _externalPool;
    std::string _metadata;
private:
    AUTIL_LOG_DECLARE();
};

END_HA3_NAMESPACE(turing);
#endif //ISEARCH_HA3AGGRESULTSVARIANT_H
