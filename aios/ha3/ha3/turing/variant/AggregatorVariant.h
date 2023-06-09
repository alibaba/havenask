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
#include "ha3/search/Aggregator.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "tensorflow/core/framework/variant_tensor_data.h"

namespace isearch {
namespace turing {

class AggregatorVariant
{
public:
    AggregatorVariant()
        : _seekedCount(0)
        , _seekTermDocCount(0)
    {}
    AggregatorVariant(const search::AggregatorPtr &aggregator,
                      const ExpressionResourcePtr &resource)
        : _aggregator(aggregator)
        , _expressionResource(resource)
        , _seekedCount(0)
        , _seekTermDocCount(0)
    {}
    ~AggregatorVariant() {}
public:
    void Encode(tensorflow::VariantTensorData* data) const {}
    bool Decode(const tensorflow::VariantTensorData& data) {
        return false;
    }
    std::string TypeName() const {
        return "Aggregator";
    }

    uint64_t getSeekedCount() const { return _seekedCount; }
    uint64_t getSeekTermDocCount() const { return _seekTermDocCount; }

    void setSeekedCount(uint64_t seekedCount) {
        _seekedCount = seekedCount;
    }
    void setSeekTermDocCount(uint64_t seekTermDocCount) {
        _seekTermDocCount = seekTermDocCount;
    }

public:
    search::AggregatorPtr getAggregator() const {
        return _aggregator;
    }
    ExpressionResourcePtr getExpressionResource() const {
        return _expressionResource;
    }
private:
    search::AggregatorPtr _aggregator;
    ExpressionResourcePtr _expressionResource;
    uint64_t _seekedCount;
    uint64_t _seekTermDocCount;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch
