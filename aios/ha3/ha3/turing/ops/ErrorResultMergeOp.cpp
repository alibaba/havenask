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
#include <iosfwd>
#include <string>
#include <memory>

#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/MultiValueVariant.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/Tracer.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;

namespace isearch {
namespace turing {

class ErrorResultMergeOp : public tensorflow::OpKernel
{
public:
    explicit ErrorResultMergeOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
        OP_REQUIRES_OK(ctx, ctx->GetAttr("error_code", &_errorCode));
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        CHECK_TIMEOUT(queryResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));
        auto commonResource = searcherQueryResource->commonResource;
        OP_REQUIRES(ctx, commonResource,
                    errors::Unavailable("ha3 common resource unavailable"));

        string errorInfo = ctx->input(0).scalar<string>()();
        if (!errorInfo.empty()) {
            commonResource->errorResult->addError(_errorCode, errorInfo);
        }
    }
private:
    int32_t _errorCode;
};

REGISTER_KERNEL_BUILDER(Name("ErrorResultMergeOp")
                        .Device(DEVICE_CPU),
                        ErrorResultMergeOp);

} // namespace turing
} // namespace isearch
