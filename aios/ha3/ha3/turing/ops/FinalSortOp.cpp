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
#include <memory>

#include "autil/Log.h" // IWYU pragma: keep
#include "tensorflow/core/framework/fbs_tensor_generated.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "turing_ops_util/variant/SortExprMeta.h"

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

namespace isearch {
namespace turing {

/*REGISTER_OP("FinalSortOp")
.Input("integers: int32")
//.Output("matchdocs: variant");
.Output("result: int64");*/

class FinalSortOp : public tensorflow::OpKernel
{
public:
    explicit FinalSortOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        Tensor* out = nullptr;

        MatchDocsVariant matchDocs;
        for (auto i = 0; i < 10; i++) {
            matchDocs.allocate(i);
        }

        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {1}, &out));
        out->scalar<Variant>()() = matchDocs;
        }

private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, FinalSortOp);

REGISTER_KERNEL_BUILDER(Name("FinalSortOp")
                        .Device(DEVICE_CPU),
                        FinalSortOp);

} // namespace turing
} // namespace isearch
