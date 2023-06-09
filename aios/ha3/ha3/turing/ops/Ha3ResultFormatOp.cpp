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
#include <string>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h" // IWYU pragma: keep
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
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/AggregateResult.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/Hits.h"
#include "ha3/common/Request.h"
#include "ha3/common/Result.h"
#include "ha3/common/Tracer.h"
#include "ha3/common/XMLResultFormatter.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/qrs/MatchDocs2Hits.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/Ha3ResultVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

using namespace isearch::config;
using namespace isearch::qrs;
using namespace isearch::common;
namespace isearch {
namespace turing {

class Ha3ResultFormatOp : public tensorflow::OpKernel
{
public:
    explicit Ha3ResultFormatOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {     
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
        isearch::service::SearcherResourcePtr searcherResource = searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource, 
                    errors::Unavailable("ha3 searcher resource unavailable"));
        ConfigAdapterPtr configAdapter = searcherSessionResource->configAdapter;
        OP_REQUIRES(ctx, configAdapter, errors::Unavailable("ha3 config adapter unavailable"));


        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
 
        auto resultTensor = ctx->input(1).scalar<Variant>()();
        auto ha3ResultVariant = resultTensor.get<Ha3ResultVariant>();
        ResultPtr result = ha3ResultVariant->getResult();
        OP_REQUIRES(ctx, result, errors::Unavailable("ha3 result unavailable"));
        vector<string> clusterNames;
        if (!configAdapter->getClusterNames(clusterNames)) {
            OP_REQUIRES(ctx, false, errors::Unavailable("get cluster name failed."));
        }
        convertMatchDocsToHits(request, result, clusterNames);
        convertAggregateResults(request, result);
        XMLResultFormatter xmlResultFormatter;
        stringstream ss;
        xmlResultFormatter.format(result, ss);

        Tensor* out = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {1}, &out));
        out->scalar<string>()() = ss.str();
    }

private:
    void convertMatchDocsToHits(const common::RequestPtr &requestPtr,
                                const ResultPtr &resultPtr,
                                const vector<string> &clusterNameVec)
    {
        if (!requestPtr || !resultPtr) {
            return;
        }

        MatchDocs2Hits converter;
        ErrorResult errResult;
        Hits *hits = converter.convert(resultPtr->getMatchDocs(),
                requestPtr, resultPtr->getSortExprMeta(),
                errResult, clusterNameVec);
        if (errResult.hasError()) {
            resultPtr->addErrorResult(errResult);
            resultPtr->clearMatchDocs();
            return ;
        }

        uint32_t actualMatchDocs = resultPtr->getActualMatchDocs();
        ConfigClause *configClause = requestPtr->getConfigClause();
        if (actualMatchDocs < configClause->getActualHitsLimit()) {
            AUTIL_LOG(DEBUG, "actual_hits_limit: %u, actual: %u, total: %u",
                    configClause->getActualHitsLimit(), actualMatchDocs,
                    resultPtr->getTotalMatchDocs());
            hits->setTotalHits(actualMatchDocs);
        } else {
            hits->setTotalHits(resultPtr->getTotalMatchDocs());
        }
        resultPtr->setHits(hits);
        resultPtr->clearMatchDocs();
    }

    void convertAggregateResults(const common::RequestPtr &requestPtr,
                                 const ResultPtr &resultPtr)
    {
        if (!requestPtr || !resultPtr) {
            return;
        }
        uint32_t aggResultCount = resultPtr->getAggregateResultCount();
        for (uint32_t i = 0; i < aggResultCount; ++i) {
            AggregateResultPtr aggResult = resultPtr->getAggregateResult(i);
            if (aggResult) {
                aggResult->constructGroupValueMap();
            }
        }
    }


private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, Ha3ResultFormatOp);

REGISTER_KERNEL_BUILDER(Name("Ha3ResultFormatOp")
                        .Device(DEVICE_CPU),
                        Ha3ResultFormatOp);

} // namespace turing
} // namespace isearch
