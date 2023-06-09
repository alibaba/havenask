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
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/mem_pool/MemoryChunk.h"
#include "ha3/common/AggregateResult.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "ha3/qrs/MatchDocs2Hits.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
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

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

using namespace isearch::config;
using namespace isearch::qrs;
using namespace isearch::common;
namespace isearch {
namespace turing {

class Ha3ResultSplitOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultSplitOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
 
        auto resultTensor = ctx->input(1).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));

        MatchDocs *matchDocs = result->getMatchDocs();
        OP_REQUIRES(ctx, matchDocs, errors::Unavailable("matchDocs unavailable"));
        auto ha3AllocatorPtr = matchDocs->getMatchDocAllocator();
        
        matchdoc::MatchDocAllocatorPtr allocatorPtr =
            dynamic_pointer_cast<matchdoc::MatchDocAllocator>(ha3AllocatorPtr);
        Pool *pool = request->getPool();
        MatchDocsVariant matchDocsVariant(allocatorPtr, pool);
        
        vector<matchdoc::MatchDoc> matchDocVect;
        matchDocs->stealMatchDocs(matchDocVect);        
        matchDocsVariant.stealMatchDocs(matchDocVect);
        
        const auto *configClause = request->getConfigClause();
        OP_REQUIRES(ctx, configClause, errors::Unavailable("configClause unavailable"));
        auto requiredTopK = configClause->getStartOffset() + configClause->getHitCount();
        
        uint32_t totalMatchDocs = matchDocs->totalMatchDocs();
        uint32_t actualMatchDocs = matchDocs->actualMatchDocs();
        uint16_t srcCount = result->getSrcCount();
        
        Tensor* matchDocsOut = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocsOut));
        matchDocsOut->scalar<Variant>()() = matchDocsVariant;

        Tensor* extraDocs = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &extraDocs));
        extraDocs->scalar<uint32_t>()() = (uint32_t)requiredTopK;
        
        Tensor* totalDocs = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &totalDocs));
        totalDocs->scalar<uint32_t>()() = totalMatchDocs;        

        Tensor* actualDocs = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(3, {}, &actualDocs));
        actualDocs->scalar<uint32_t>()() = actualMatchDocs;

        Tensor* resultOutTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(4, {}, &resultOutTensor));
        resultOutTensor->scalar<Variant>()() = *ha3ResultVariant;

        Tensor* srcCountTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(5, {}, &srcCountTensor));
        srcCountTensor->scalar<uint16_t>()() = srcCount;        
    }

private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, Ha3ResultSplitOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultSplitOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultSplitOp);

} // namespace turing
} // namespace isearch
