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
#include <algorithm>
#include <iostream>
#include <memory>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "ha3/common/AggregateResult.h"
#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Result.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "matchdoc/MatchDoc.h"
#include "matchdoc/MatchDocAllocator.h"
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

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;
namespace isearch {
namespace turing {

class Ha3ResultReconstructOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultReconstructOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto resultTensor = ctx->input(0).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));
        
        auto matchdocsTensor = ctx->input(1).scalar<Variant>()();
        auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant, 
                    errors::Unavailable("xxxxx ha3 searcher resource unavailable"));

        uint32_t totalMatchDocs = ctx->input(2).scalar<uint32_t>()();
        uint32_t actualMatchDocs = ctx->input(3).scalar<uint32_t>()();        

        matchdoc::MatchDocAllocatorPtr allocator = matchDocsVariant->getAllocator();
        Ha3MatchDocAllocatorPtr ha3Allocator = 
            dynamic_pointer_cast<Ha3MatchDocAllocator>(allocator);
        OP_REQUIRES(ctx, ha3Allocator, errors::Unavailable("dynamic_pointer_cast allocator failed."));

        MatchDocs *matchDocs = result->getMatchDocs();
        vector<matchdoc::MatchDoc> matchDocVec = matchDocsVariant->getMatchDocs();
        if (matchDocs) {
            matchDocs->getMatchDocsVect() = matchDocVec;
            matchDocs->setMatchDocAllocator(ha3Allocator);
            matchDocs->setTotalMatchDocs(totalMatchDocs);
            matchDocs->setActualMatchDocs(actualMatchDocs);
        }

        result->setSortExprMeta(matchDocsVariant->getSortMetas());
        Tensor* resultOutTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &resultOutTensor));
        resultOutTensor->scalar<Variant>()() = *ha3ResultVariant;
        
    }

private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, Ha3ResultReconstructOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultReconstructOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultReconstructOp);

} // namespace turing
} // namespace isearch
