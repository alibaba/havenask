#include <ha3/turing/variant/AggregateResultsVariant.h>
#include <tensorflow/core/framework/variant_op_registry.h>
#include <autil/DataBuffer.h>

using namespace std;
using namespace tensorflow;
using namespace autil;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(common);
BEGIN_HA3_NAMESPACE(turing);

HA3_LOG_SETUP(turing, AggregateResultsVariant);

AggregateResultsVariant::AggregateResultsVariant()
    : _toAggCount(0)
    , _aggCount(0)
    , _seekedCount(0)
    , _seekTermDocCount(0)
    , _externalPool(nullptr)
{
}

AggregateResultsVariant::AggregateResultsVariant(HA3_NS(common)::AggregateResultsPtr aggResults,
        autil::mem_pool::Pool *pool)
    : _aggResults(aggResults)
    , _toAggCount(0)
    , _aggCount(0)
    , _seekedCount(0)
    , _seekTermDocCount(0)
    , _externalPool(nullptr)
{
}
AggregateResultsVariant::AggregateResultsVariant(HA3_NS(common)::AggregateResultsPtr aggResults,
        uint64_t toAggCount,
        uint64_t aggCount,
        autil::mem_pool::Pool *pool)
    : _aggResults(aggResults)
    , _toAggCount(toAggCount)
    , _aggCount(aggCount)
    , _seekedCount(0)
    , _seekTermDocCount(0)
    , _externalPool(nullptr)
{
}

AggregateResultsVariant::AggregateResultsVariant(const AggregateResultsVariant &other)
    : _aggResults(other._aggResults)
    , _toAggCount(other._toAggCount)
    , _aggCount(other._aggCount)
    , _seekedCount(other._seekedCount)
    , _seekTermDocCount(other._seekTermDocCount)
    , _externalPool(other._externalPool)
    , _metadata(other._metadata)
{
}

AggregateResultsVariant::~AggregateResultsVariant() {
    _aggResults.reset();
    _externalPool = nullptr;
}

void AggregateResultsVariant::Encode(VariantTensorData* data) const {
    data->set_type_name(TypeName());
    DataBuffer dataBuffer(DataBuffer::DEFAUTL_DATA_BUFFER_SIZE, _externalPool);
    dataBuffer.write(_aggResults->size());
    for(auto aggResult: *_aggResults) {
        aggResult->serialize(dataBuffer);
    }
    data->metadata_.assign(dataBuffer.getData(), dataBuffer.getDataLen());
}

bool AggregateResultsVariant::Decode(const VariantTensorData& data) {
    if (TypeName() != data.type_name()) {
        return false;
    }
    _metadata = data.metadata_;
    return true;
}

bool AggregateResultsVariant::construct(autil::mem_pool::Pool *outPool) {
    DataBuffer dataBuffer((void*)_metadata.c_str(), _metadata.size(), outPool);
    _aggResults.reset(new AggregateResults);
    size_t aggResultCount = 0;
    dataBuffer.read(aggResultCount);
    for(size_t i = 0; i < aggResultCount; i++){
        AggregateResultPtr aggResult(new AggregateResult());
        aggResult->deserialize(dataBuffer, outPool);
        _aggResults->push_back(aggResult);
    }
    return true;
}

REGISTER_UNARY_VARIANT_DECODE_FUNCTION(AggregateResultsVariant, "Ha3AggregateResults");

END_HA3_NAMESPACE(turing);
