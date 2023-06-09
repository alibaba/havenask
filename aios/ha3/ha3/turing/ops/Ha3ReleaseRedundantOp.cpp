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
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "turing_ops_util/util/OpUtil.h"
#include "indexlib/index_define.h"
#include "indexlib/misc/common.h"
#include "matchdoc/MatchDoc.h"
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

#include "ha3/common/Ha3MatchDocAllocator.h"
#include "ha3/common/MatchDocs.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

class Ha3ReleaseRedundantOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ReleaseRedundantOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        OP_REQUIRES(ctx, sessionResource, 
                    errors::Unavailable("session resource unavailable"));

        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherQueryResource *searcherQueryResource = 
            dynamic_cast<SearcherQueryResource *>(queryResource);
        OP_REQUIRES(ctx, searcherQueryResource, 
                    errors::Unavailable("ha3 searcher query resource unavailable"));

        SearchRuntimeResourcePtr runtimeResource = searcherQueryResource->runtimeResource;
        OP_REQUIRES(ctx, runtimeResource, 
                    errors::Unavailable("ha3 runtime resource unavailable"));

        auto requiredTopK = runtimeResource->docCountLimits.requiredTopK;
        auto resultTensor = ctx->input(0).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));

        
        MatchDocs *matchDocs = result->getMatchDocs();
        if (matchDocs) {
            auto ha3MatchDocAllocator = matchDocs->getMatchDocAllocator();
            if (ha3MatchDocAllocator) {
                std::vector<matchdoc::MatchDoc> &matchDocVect = matchDocs->getMatchDocsVect();
                releaseRedundantMatchDocs(matchDocVect, requiredTopK, ha3MatchDocAllocator);
            }
        }
        ctx->set_output(0, ctx->input(0));
    }

private:
    void releaseRedundantMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocVect,
                                   uint32_t requiredTopK,
                                   const common::Ha3MatchDocAllocatorPtr &matchDocAllocator) {
        uint32_t size = (uint32_t)matchDocVect.size();
        while(size > requiredTopK)
        {
            auto matchDoc = matchDocVect[--size];
            matchDocAllocator->deallocate(matchDoc);
        }
        matchDocVect.erase(matchDocVect.begin() + size, matchDocVect.end());        
    }

private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, Ha3ReleaseRedundantOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ReleaseRedundantOp")
                        .Device(DEVICE_CPU),
                        Ha3ReleaseRedundantOp);

} // namespace turing
} // namespace isearch
