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
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/search/Aggregator.h"
#include "ha3/turing/variant/AggregatorVariant.h"
#include "suez/turing/expression/common.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"

using namespace tensorflow;
using namespace suez::turing;

namespace isearch {
namespace turing {

class AggregatorOp : public tensorflow::OpKernel
{
public:
    explicit AggregatorOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto aggregatorTensor = ctx->input(0).scalar<Variant>()();
        auto aggregatorVariant = aggregatorTensor.get<AggregatorVariant>();
        OP_REQUIRES(ctx, aggregatorVariant, errors::Unavailable("get aggregator failed"));

        auto matchDocsTensor = ctx->input(1).scalar<Variant>()();
        auto matchDocsVariant = matchDocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant, errors::Unavailable("get matchDocs failed"));

        auto aggregator = aggregatorVariant->getAggregator();
        OP_REQUIRES(ctx, aggregator, errors::Unavailable("aggregator is empty."));

        aggregator->batchAggregate(matchDocsVariant->getMatchDocs());
        ctx->set_output(0, ctx->input(0));
        ctx->set_output(1, ctx->input(1));
    }
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, AggregatorOp);

REGISTER_KERNEL_BUILDER(Name("AggregatorOp")
                        .Device(DEVICE_CPU),
                        AggregatorOp);

} // namespace turing
} // namespace isearch
