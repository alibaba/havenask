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

#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/config/ServiceDegradationConfig.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::service;
using namespace isearch::common;
using namespace isearch::config;
namespace isearch {
namespace turing {

class AddDegradeInfoOp : public tensorflow::OpKernel
{
public:
    explicit AddDegradeInfoOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherSessionResource *searcherSessionResource =
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherSessionResource,
                    errors::Unavailable("ha3 searcher session resource unavailable"));
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));

        float serviceDegradationLevel = queryResource->getDegradeLevel();
        if (serviceDegradationLevel <= 0) {
            ctx->set_output(0, ctx->input(0));
            return;
        }

        isearch::service::SearcherResourcePtr searcherResource =
            searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource,
                    errors::Unavailable("ha3 searcher resource unavailable"));

        isearch::service::ServiceDegradePtr serviceDegrade = searcherResource->getServiceDegrade();
        if (!serviceDegrade) {
            ctx->set_output(0,ctx->input(0));
            return;
        }

        isearch::config::ServiceDegradationConfig serviceDegradationConfig =
            serviceDegrade->getServiceDegradationConfig();
        uint32_t rankSize = serviceDegradationConfig.request.rankSize;
        uint32_t rerankSize = serviceDegradationConfig.request.rerankSize;

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
        request->setDegradeLevel(serviceDegradationLevel,rankSize,rerankSize);

        auto metricsCollector =  searcherQueryResource->sessionMetricsCollector;
        if (metricsCollector) {
            metricsCollector->setDegradeLevel(serviceDegradationLevel);
            metricsCollector->increaseServiceDegradationQps();
        }

        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<Variant>()() = *ha3RequestVariant;
    }

private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, AddDegradeInfoOp);

REGISTER_KERNEL_BUILDER(Name("AddDegradeInfoOp")
                        .Device(DEVICE_CPU),
                        AddDegradeInfoOp);

} // namespace turing
} // namespace isearch
