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
#include <iosfwd>
#include <memory>
#include <type_traits>
#include <utility>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/isearch.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/KvPairVariant.h"

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;

namespace isearch {
namespace turing {

class Ha3RequestToKvPairOp : public tensorflow::OpKernel
{
public:
    explicit Ha3RequestToKvPairOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        CHECK_TIMEOUT(queryResource);

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto requestVariant = requestTensor.get<Ha3RequestVariant>();
        OP_REQUIRES(ctx, requestVariant, errors::Unavailable("get requestVariant failed"));
        RequestPtr request = requestVariant->getRequest();
        ConfigClause *configClause = request->getConfigClause();
        OP_REQUIRES(ctx, configClause, errors::Unavailable("configClause is nullptr"));
        KeyValueMapPtr kvPairs = configClause->getKeyValueMapPtr();
        OP_REQUIRES(ctx, kvPairs, errors::Unavailable("KeyValueMap is nullptr"));
        KvPairVariant kvPairVariant(kvPairs, queryResource->getPool());

        Tensor* tensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &tensor));
        tensor->scalar<Variant>()() = move(kvPairVariant);
    }
};

REGISTER_KERNEL_BUILDER(Name("Ha3RequestToKvPairOp")
                        .Device(DEVICE_CPU),
                        Ha3RequestToKvPairOp);

} // namespace turing
} // namespace isearch
