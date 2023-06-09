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

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/config/AggSamplerConfigInfo.h"
#include "ha3/search/Aggregator.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "suez/turing/common/QueryResource.h"
#include "tensorflow/core/framework/op_kernel.h"

namespace isearch {
namespace turing {

class AggPrepareOp : public tensorflow::OpKernel
{
public:
    explicit AggPrepareOp(tensorflow::OpKernelConstruction* ctx);
public:
    void Compute(tensorflow::OpKernelContext* ctx) override;

    static search::AggregatorPtr createAggregator(const ExpressionResourcePtr &resource,
            suez::turing::QueryResource *queryResource,
            const config::AggSamplerConfigInfo &configInfo = config::AggSamplerConfigInfo());
private:
    config::AggSamplerConfigInfo _aggSamplerConfig;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace turing
} // namespace isearch

