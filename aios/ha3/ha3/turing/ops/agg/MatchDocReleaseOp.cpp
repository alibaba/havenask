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
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h" // IWYU pragma: keep
#include "matchdoc/MatchDocAllocator.h"
#include "tensorflow/core/framework/fbs_tensor_generated.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"
#include "turing_ops_util/variant/SortExprMeta.h"

namespace matchdoc {
class MatchDoc;
}  // namespace matchdoc

using namespace tensorflow;
using namespace suez::turing;

namespace isearch {
namespace turing {

class MatchDocReleaseOp : public tensorflow::OpKernel
{
public:
    explicit MatchDocReleaseOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto matchDocsTensor = ctx->input(1).scalar<Variant>()();
        auto matchDocsVariant = matchDocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant, errors::Unavailable("get matchDocs failed"));
        std::vector<matchdoc::MatchDoc> &matchDocs = matchDocsVariant->getMatchDocs();
        auto allocator = matchDocsVariant->getAllocator();
        for (size_t i = 0; i < matchDocs.size(); ++i) {
            allocator->deallocate(matchDocs[i]);
        }
        matchDocs.clear();
        bool eof = ctx->input(0).scalar<bool>()();
        AUTIL_LOG(WARN, "seek end [%d]", eof);

        ctx->set_output(0, ctx->input(0));
    }
private:
    AUTIL_LOG_DECLARE();
};

AUTIL_LOG_SETUP(ha3, MatchDocReleaseOp);

REGISTER_KERNEL_BUILDER(Name("MatchDocReleaseOp")
                        .Device(DEVICE_CPU),
                        MatchDocReleaseOp);

} // namespace turing
} // namespace isearch
