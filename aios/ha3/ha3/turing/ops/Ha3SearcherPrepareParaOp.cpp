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
#include "ha3/turing/ops/Ha3SearcherPrepareParaOp.h"

#include <iosfwd>
#include <memory>
#include <memory>

#include "autil/TimeoutTerminator.h"
#include "turing_ops_util/util/OpUtil.h"
#include "suez/turing/common/QueryResource.h"
#include "suez/turing/common/RuntimeState.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/framework/AttributeExpressionCreator.h"
#include "tensorflow/core/framework/kernel_def_builder.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor_types.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/variant.h"
#include "tensorflow/core/lib/core/errors.h"
#include "tensorflow/core/lib/core/stringpiece.h"
#include "tensorflow/core/platform/byte_order.h"

#include "ha3/common/Request.h"
#include "ha3/common/TimeoutTerminator.h"
#include "ha3/common/Tracer.h"
#include "ha3/monitor/SessionMetricsCollector.h"
#include "ha3/search/IndexPartitionReaderWrapper.h"
#include "ha3/search/IndexPartitionReaderUtil.h"
#include "ha3/search/LayerMetas.h"
#include "ha3/search/SearchCommonResource.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/turing/ops/Ha3ResourceUtil.h"
#include "ha3/turing/ops/RequestUtil.h"
#include "ha3/turing/ops/SearcherQueryResource.h"
#include "ha3/turing/ops/SearcherSessionResource.h"
#include "ha3/turing/variant/Ha3RequestVariant.h"
#include "ha3/turing/variant/LayerMetasVariant.h"
#include "ha3/turing/variant/SeekResourceVariant.h"
#include "autil/Log.h"

using namespace std;
using namespace tensorflow;

using namespace isearch::common;
using namespace isearch::monitor;
using namespace isearch::search;
namespace isearch {
namespace turing {
AUTIL_LOG_SETUP(ha3, Ha3SearcherPrepareParaOp);

void Ha3SearcherPrepareParaOp::Compute(tensorflow::OpKernelContext* ctx) {
        auto sessionResource = GET_SESSION_RESOURCE();
        SearcherSessionResource *searcherSessionResource =
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        OP_REQUIRES(ctx, searcherSessionResource,
                    errors::Unavailable("ha3 searcher session resource "
                            "unavailable"));
        auto queryResource = GET_QUERY_RESOURCE(sessionResource);
        SearcherQueryResource *searcherQueryResource =
            dynamic_cast<SearcherQueryResource *>(queryResource);

        OP_REQUIRES(ctx, searcherQueryResource,
                    errors::Unavailable("ha3 searcher query resource unavailable"));
        isearch::service::SearcherResourcePtr searcherResource =
            searcherSessionResource->searcherResource;
        OP_REQUIRES(ctx, searcherResource,
                    errors::Unavailable("ha3 searcher resource unavailable"));

        auto requestTensor = ctx->input(0).scalar<Variant>()();
        auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
        RequestPtr request = ha3RequestVariant->getRequest();
        OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
        OP_REQUIRES(ctx, RequestUtil::defaultSorterBeginRequest(request),
                    errors::Unavailable("defaultSorter BeginReuest fail"));

        auto layerMetasTensor = ctx->input(1).scalar<Variant>()();
        auto layerMetasVariant = layerMetasTensor.get<LayerMetasVariant>();
        LayerMetasPtr layerMetas = layerMetasVariant->getLayerMetas();
        OP_REQUIRES(ctx, layerMetas, errors::Unavailable("layermetas unavailable"));

        // 1. common resource
        SeekResourcePtr seekResource(new SeekResource);
        auto pool = searcherQueryResource->getPool();
        auto metricsCollector = SessionMetricsCollectorPtr(
                searcherQueryResource->sessionMetricsCollector->cloneWithoutTracer());
        auto tracer = searcherQueryResource->getTracer();
        TracerPtr tracerNew;
        if (tracer) {
            tracerNew.reset(tracer->cloneWithoutTraceInfo());
        }

        metricsCollector->setTracer(tracerNew.get());
        seekResource->setTracer(tracerNew);
        seekResource->setMetricsCollector(metricsCollector);
        REQUEST_TRACE_WITH_TRACER(tracerNew, TRACE3,
                "===== begin parallel seek =====");

        TimeoutTerminatorPtr timeoutTerminator(
                new TimeoutTerminator(
                        *searcherQueryResource->getSeekTimeoutTerminator()));
        seekResource->setTimeoutTerminator(timeoutTerminator);
        SearchCommonResourcePtr commonResource =
            Ha3ResourceUtil::createSearchCommonResource(
                    request.get(), pool, metricsCollector.get(),
                    timeoutTerminator.get(),
                    tracerNew.get(), searcherResource,
                    searcherQueryResource->getCavaAllocator(),
                    searcherQueryResource->getCavaJitModulesWrapper());
        OP_REQUIRES(ctx, commonResource,
                    errors::Unavailable("create common resource failed"));


        auto partitionReaderSnapshot = searcherQueryResource->getIndexSnapshot();
        OP_REQUIRES(ctx, partitionReaderSnapshot,
                    errors::Unavailable("partitionReaderSnapshot unavailable"));
        IndexPartitionReaderWrapperPtr idxPartReaderWrapperPtr =
            IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(partitionReaderSnapshot,
                    searcherSessionResource->mainTableSchemaName);

        OP_REQUIRES(ctx, idxPartReaderWrapperPtr, errors::Unavailable("ha3 index "
                        "partition reader wrapper unavailable"));

        // 2. parition resource
        SearchPartitionResourcePtr partitionResource =
            Ha3ResourceUtil::createSearchPartitionResource(request.get(),
                    idxPartReaderWrapperPtr, partitionReaderSnapshot,
                    commonResource);
        OP_REQUIRES(ctx, partitionResource, errors::Unavailable("create partition "
                        "resource failed"));

        // 3. runtime resource
        SearchRuntimeResourcePtr runtimeResource =
            Ha3ResourceUtil::createSearchRuntimeResource(request.get(),
                    searcherResource, commonResource,
                    partitionResource->attributeExpressionCreator.get(),
                    searcherQueryResource->totalPartCount);
        OP_REQUIRES(ctx, runtimeResource, errors::Unavailable("create runtime "
                        "resource failed"));

        seekResource->commonResource = commonResource;
        seekResource->partitionResource = partitionResource;
        seekResource->runtimeResource = runtimeResource;
        seekResource->layerMetas = layerMetas;

        RequestPtr requestClone(request->clone(pool));
        Ha3RequestVariant requestVariantOut(requestClone, pool);
        Tensor* requestTensorOut = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &requestTensorOut));
        requestTensorOut->scalar<tensorflow::Variant>()() = requestVariantOut;

        SeekResourceVariant seekResourceVariant(seekResource);
        Tensor* seekResourceTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &seekResourceTensor));
        seekResourceTensor->scalar<tensorflow::Variant>()() = seekResourceVariant;
}

REGISTER_KERNEL_BUILDER(Name("Ha3SearcherPrepareParaOp")
                        .Device(DEVICE_CPU),
                        Ha3SearcherPrepareParaOp);

} // namespace turing
} // namespace isearch
