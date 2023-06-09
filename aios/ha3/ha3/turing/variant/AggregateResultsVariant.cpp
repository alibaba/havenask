/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/turing/variant/AggregateResultsVariant.h"

#include <cstddef>
#include <memory>
#include <vector>

#include "autil/DataBuffer.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/AggregateResult.h"
#include "tensorflow/core/framework/variant_op_registry.h"
#include "tensorflow/core/platform/byte_order.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace tensorflow;
using namespace autil;
using namespace autil::mem_pool;

using namespace isearch::common;
namespace isearch {
namespace turing {

AUTIL_LOG_SETUP(ha3, AggregateResultsVariant);

AggregateResultsVariant::AggregateResultsVariant()
    : _toAggCount(0)
    , _aggCount(0)
    , _seekedCount(0)
    , _seekTermDocCount(0)
    , _externalPool(nullptr)
{
}

AggregateResultsVariant::AggregateResultsVariant(isearch::common::AggregateResultsPtr aggResults,
        autil::mem_pool::Pool *pool)
    : _aggResults(aggResults)
    , _toAggCount(0)
    , _aggCount(0)
    , _seekedCount(0)
    , _seekTermDocCount(0)
    , _externalPool(nullptr)
{
}
AggregateResultsVariant::AggregateResultsVariant(isearch::common::AggregateResultsPtr aggResults,
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

} // namespace turing
} // namespace isearch
