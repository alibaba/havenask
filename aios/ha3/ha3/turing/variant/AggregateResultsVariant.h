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
#pragma once

#include <stdint.h>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "tensorflow/core/framework/variant_encode_decode.h" // IWYU pragma: keep
#include "tensorflow/core/framework/variant_tensor_data.h"

#include "ha3/common/AggregateResult.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil


namespace isearch {
namespace turing {
class AggregateResultsVariant
{
public:
    AggregateResultsVariant();
    AggregateResultsVariant(isearch::common::AggregateResultsPtr aggResults,
                            autil::mem_pool::Pool *pool);
    AggregateResultsVariant(isearch::common::AggregateResultsPtr aggResults,
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
    isearch::common::AggregateResultsPtr getAggResults() const { return _aggResults; }
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
    isearch::common::AggregateResultsPtr _aggResults;
    uint64_t _toAggCount;
    uint64_t _aggCount;
    uint64_t _seekedCount;
    uint64_t _seekTermDocCount;
    autil::mem_pool::Pool* _externalPool;
    std::string _metadata;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
