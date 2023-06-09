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
#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/AggregateResult.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/proxy/Merger.h"
#include "ha3/search/Aggregator.h"
#include "ha3/turing/variant/AggregateResultsVariant.h"
#include "ha3/turing/variant/AggregatorVariant.h"
#include "ha3/turing/variant/ExpressionResourceVariant.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/util/OpUtil.h"

namespace isearch {
namespace common {
class AggregateClause;
}  // namespace common
}  // namespace isearch

using namespace tensorflow;
using namespace suez::turing;
using namespace isearch::common;

namespace isearch {
namespace turing {

class AggMergeOp : public tensorflow::OpKernel {
public:
    explicit AggMergeOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {}
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        OpInputList aggregators;
        OP_REQUIRES_OK(ctx, ctx->input_list("aggregators", &aggregators));
        const int n = aggregators.size();
        std::vector<common::AggregateResults> results;
        AggregateClause *aggClause = NULL;
        uint64_t aggCount = 0;
        uint64_t toAggCount = 0;
        uint64_t seekedCount = 0;
        uint64_t seekTermDocCount = 0;
        for (int i = 0; i < n; i++) {
            auto aggregatorVariant = aggregators[i].scalar<Variant>()().get<AggregatorVariant>();
            if (!aggregatorVariant) {
                continue;
            }
            auto aggregator = aggregatorVariant->getAggregator();
            if (!aggregator) {
                continue;
            }
            seekedCount += aggregatorVariant->getSeekedCount();
            seekTermDocCount += aggregatorVariant->getSeekTermDocCount();
            aggregator->endLayer(1.0);
            aggregator->estimateResult(1.0);
            aggCount += aggregator->getAggregateCount();
            toAggCount += aggregator->getToAggDocCount();
            if (!aggClause) {
                ExpressionResourcePtr resource = aggregatorVariant->getExpressionResource();
                aggClause = resource->_request->getAggregateClause();
            }
            auto aggregateResultsPtr = aggregator->collectAggregateResult();
            results.push_back(*aggregateResultsPtr);
        }

        // merge
        common::AggregateResultsPtr aggResultsPtr(new AggregateResults());
        if (results.size() == 1) {
            *aggResultsPtr = results[0];
        } else if (results.size() > 1) {
            proxy::Merger::mergeAggResults(*aggResultsPtr, results, aggClause, queryResource->getPool());
        }
        // output
        AggregateResultsVariant aggResultsVariant(aggResultsPtr, toAggCount,
                aggCount, queryResource->getPool());
        aggResultsVariant.setSeekedCount(seekedCount);
        aggResultsVariant.setSeekTermDocCount(seekTermDocCount);
        Tensor *outputTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
        outputTensor->scalar<Variant>()() = aggResultsVariant;
    }
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, AggMergeOp);

REGISTER_KERNEL_BUILDER(Name("AggMergeOp")
                        .Device(DEVICE_CPU),
                        AggMergeOp);

} // namespace turing
} // namespace isearch
