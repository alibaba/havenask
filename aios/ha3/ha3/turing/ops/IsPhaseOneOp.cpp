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
#include <stdint.h>
#include <iostream>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "turing_ops_util/util/OpUtil.h"
#include "suez/turing/common/PoolConfig.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::monitor;
namespace isearch {
namespace turing {

class IsPhaseOneOp : public tensorflow::OpKernel
{
public:
    explicit IsPhaseOneOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("searcher query resource unavailable."));
        auto metricsCollector = searcherQueryResource->sessionMetricsCollector;;
        OP_REQUIRES(ctx, metricsCollector,
                    errors::Unavailable("metrics collector unavailable."));
        metricsCollector->graphRunStartTrigger();

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
        uint32_t phaseNumber = request->getConfigClause()->getPhaseNumber();
        bool isPhaseOne = phaseNumber == SEARCH_PHASE_ONE;
        if (isPhaseOne) {
            metricsCollector->setRequestType(SessionMetricsCollector::IndependentPhase1);
        } else {
            metricsCollector->setRequestType(SessionMetricsCollector::IndependentPhase2);
        }

        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<bool>()() = isPhaseOne;
    }
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, IsPhaseOneOp);

REGISTER_KERNEL_BUILDER(Name("IsPhaseOneOp")
                        .Device(DEVICE_CPU),
                        IsPhaseOneOp);

} // namespace turing
} // namespace isearch
