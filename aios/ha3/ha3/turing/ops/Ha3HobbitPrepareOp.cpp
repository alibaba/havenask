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
#include <map>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "ha3/common/ClauseBase.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/Request.h"
#include "ha3/common/SortClause.h"
#include "ha3/common/SortDescription.h"
#include "ha3/common/Tracer.h"
#include "ha3/common/VirtualAttributeClause.h"
#include "ha3/search/DocCountLimits.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/util/VirtualAttribute.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"
#include "turing_ops_util/util/OpUtil.h"
#include "turing_ops_util/variant/KvPairVariant.h"
#include "turing_ops_util/variant/MatchDocsVariant.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil

using namespace tensorflow;
using namespace suez::turing;
using namespace autil;
using namespace std;
using namespace autil;

using namespace isearch::common;
using namespace isearch::search;

namespace isearch {
namespace turing {

class Ha3HobbitPrepareOp : public tensorflow::OpKernel
{
public:
    explicit Ha3HobbitPrepareOp(tensorflow::OpKernelConstruction* ctx)
        : tensorflow::OpKernel(ctx)
    {
    }
public:
    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        OP_REQUIRES(ctx, queryResource,
                    errors::Unavailable("ha3 query resource unavailable"));

        // get request
        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        OP_REQUIRES(ctx, ha3RequestVariant, errors::Unavailable("get request variant failed"));

        auto matchdocsTensor = ctx->input(1).scalar<Variant>()();
        auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();
        OP_REQUIRES(ctx, matchDocsVariant, errors::Unavailable("get matchDocsVariant failed"));

        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("get request failed"));

        // prepare ConfigClause
        ConfigClause *configClause = request->getConfigClause();
        OP_REQUIRES(ctx, configClause, errors::Unavailable("get config clause failed"));

        // prepare KvPairVariant
        std::map<std::string, std::string> kvPair = configClause->getKVPairs();
        kvPair["s"] = StringUtil::toString(configClause->getStartOffset());
        kvPair["n"] = StringUtil::toString(configClause->getHitCount());
        if (DocCountLimits::determinScorerId(request.get()) == 1) {
            kvPair["HPS"] = string(suez::turing::BUILD_IN_REFERENCE_PREFIX) + "score";
        }

        auto sortClause = request->getSortClause();
        if (sortClause) {
            auto ssDesc = sortClause->getSortDescription(1u);
            if (ssDesc) {
                string ssName = ssDesc->getOriginalString();
                kvPair["HSS"] = ssName;
                auto virtualAttributeClause = request->getVirtualAttributeClause();
                if (virtualAttributeClause) {
                    auto va = virtualAttributeClause->getVirtualAttribute(ssName);
                    if (va) {
                        string exprStr = va->getExprString();
                        matchDocsVariant->addAliasField(ssName, exprStr);
                    }
                }
            }
        }

        int32_t level = Tracer::convertLevel(configClause->getTrace());

        bool debug = level <= ISEARCH_TRACE_DEBUG ? true : false;
        int  debugLevel = 0;
        switch(level) {
            case ISEARCH_TRACE_ALL:
            case ISEARCH_TRACE_TRACE3: debugLevel = 3; break;
            case ISEARCH_TRACE_TRACE2: debugLevel = 2; break;
            case ISEARCH_TRACE_TRACE1: debugLevel = 1; break;
            default: debugLevel = 0;
        }
        kvPair["debug"] = debug ? "true" : "false";
        kvPair["debuglevel"] = StringUtil::toString(debugLevel);
        autil::mem_pool::Pool *pool = queryResource->getPool();
        KvPairVariant kvPairVariant(kvPair, pool);

        Tensor *kvPairTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &kvPairTensor));
        kvPairTensor->scalar<Variant>()() = kvPairVariant;


        Tensor* matchDocs = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &matchDocs));
        matchDocs->scalar<Variant>()() = *matchDocsVariant;
    }
private:
    AUTIL_LOG_DECLARE();
};


AUTIL_LOG_SETUP(ha3, Ha3HobbitPrepareOp);

REGISTER_KERNEL_BUILDER(Name("Ha3HobbitPrepareOp")
                        .Device(DEVICE_CPU),
                        Ha3HobbitPrepareOp);

} // namespace truing
} // namespace isearch
