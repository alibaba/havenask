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
#include <iostream>
#include <memory>
#include <memory>

#include "turing_ops_util/util/OpUtil.h"
#include "indexlib/misc/common.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/op_kernel.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_shape.pb.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"

#include "ha3/common/AttributeItem.h"
#include "ha3/common/Request.h"
#include "ha3/common/Tracer.h"
#include "ha3/config/JoinConfig.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/turing/ops/Ha3ResourceUtil.h"
#include "ha3/turing/ops/RequestUtil.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "autil/Log.h" // IWYU pragma: keep

using namespace std;
using namespace tensorflow;

using namespace isearch::rank;
using namespace isearch::service;
using namespace isearch::common;
using namespace indexlib::index;
using namespace isearch::search;
using namespace isearch::config;
namespace isearch {
namespace turing {

class Ha3SearcherPrepareOp : public tensorflow::OpKernel
{
public:
    explicit Ha3SearcherPrepareOp(tensorflow::OpKernelConstruction* ctx) : tensorflow::OpKernel(ctx) {
    }
public:

    void Compute(tensorflow::OpKernelContext* ctx) override {
        auto sessionResource = GET_SESSION_RESOURCE();

        SearcherSessionResource *searcherSessionResource =
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        OP_REQUIRES(ctx, searcherSessionResource,
                    errors::Unavailable("ha3 searcher session resource unavailable"));

        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);

        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));
        isearch::service::SearcherResourcePtr searcherResource =
            searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource,
                    errors::Unavailable("ha3 searcher resource unavailable"));

        auto pool = searcherQueryResource->getPool();
        auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
        IndexPartitionReaderWrapperPtr idxPartReaderWrapperPtr
            = searcherQueryResource->indexPartitionReaderWrapper;
        if (idxPartReaderWrapperPtr == NULL) {
            OP_REQUIRES(ctx, idxPartReaderWrapperPtr,
                        errors::Unavailable("ha3 index partition reader wrapper unavailable"));
        }
        auto partitionReaderSnapshot = searcherQueryResource->getIndexSnapshot();
        OP_REQUIRES(ctx, partitionReaderSnapshot,
                    errors::Unavailable("partitionReaderSnapshot unavailable"));
        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

        OP_REQUIRES(ctx, RequestUtil::defaultSorterBeginRequest(request),
                    errors::Unavailable("defaultSorter BeginReuest fail"));

        SearchCommonResourcePtr commonResource =
            Ha3ResourceUtil::createSearchCommonResource(
                    request.get(), pool, metricsCollector,
                    searcherQueryResource->getSeekTimeoutTerminator(),
                    searcherQueryResource->getTracer(), searcherResource,
                    searcherQueryResource->getCavaAllocator(),
                    searcherQueryResource->getCavaJitModulesWrapper());
        OP_REQUIRES(ctx, commonResource, errors::Unavailable("create common resource failed"));

        SearchPartitionResourcePtr partitionResource =
            Ha3ResourceUtil::createSearchPartitionResource(request.get(),
                    idxPartReaderWrapperPtr, partitionReaderSnapshot,
                    commonResource);
        OP_REQUIRES(ctx, partitionResource, errors::Unavailable("create partition resource failed"));


        SearchRuntimeResourcePtr runtimeResource =
            Ha3ResourceUtil::createSearchRuntimeResource(request.get(),
                    searcherResource, commonResource,
                    partitionResource->attributeExpressionCreator.get(),
                    searcherQueryResource->totalPartCount);

        searcherQueryResource->commonResource = commonResource;
        searcherQueryResource->partitionResource = partitionResource;
        searcherQueryResource->runtimeResource = runtimeResource;
        OP_REQUIRES(ctx, runtimeResource, errors::Unavailable("create runtime resource failed"));
    }

private:
    AUTIL_LOG_DECLARE();

};

AUTIL_LOG_SETUP(ha3, Ha3SearcherPrepareOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SearcherPrepareOp")
                        .Device(DEVICE_CPU),
                        Ha3SearcherPrepareOp);

} // namespace turing
} // namespace isearch
