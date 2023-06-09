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
#include <iostream>
#include <string>
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "turing_ops_util/variant/MultiValueVariant.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/ClauseBase.h"
#include "ha3/common/FinalSortClause.h"
#include "ha3/common/Request.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;
using namespace isearch::monitor;
namespace isearch {
namespace turing {

class Ha3NeedHobbitOp : public tensorflow::OpKernel
{
public:
    explicit Ha3NeedHobbitOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
        bool needHobbit = false;
        auto finalSortClause = request->getFinalSortClause();
        if (finalSortClause && "AdapterSorter" == finalSortClause->getSortName()) {
            needHobbit = true;
        }
        
        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &out));
        out->scalar<bool>()() = needHobbit;
    }
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, Ha3NeedHobbitOp);

REGISTER_KERNEL_BUILDER(Name("Ha3NeedHobbitOp")
                        .Device(DEVICE_CPU),
                        Ha3NeedHobbitOp);

} // namespace turing
} // namespace isearch
