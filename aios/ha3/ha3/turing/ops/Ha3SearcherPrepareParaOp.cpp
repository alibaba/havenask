#include <ha3/turing/ops/Ha3SearcherPrepareParaOp.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/turing/ops/SearcherSessionResource.h>
#include <ha3/rank/RankProfileManager.h>
#include <ha3/search/MatchDocSearcher.h>
#include <ha3/search/SearchCommonResource.h>
#include <ha3/service/SearcherResource.h>
#include <ha3/common/Tracer.h>
#include <ha3/common/TimeoutTerminator.h>
#include <ha3/turing/ops/Ha3ResourceUtil.h>
#include <ha3/turing/ops/RequestUtil.h>
#include "ha3/common/ha3_op_common.h"
#include <ha3/turing/variant/SeekResourceVariant.h>
#include <ha3/turing/variant/LayerMetasVariant.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/monitor/SessionMetricsCollector.h>

using namespace std;
using namespace tensorflow;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(search);
BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, Ha3SearcherPrepareParaOp);

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
        HA3_NS(service)::SearcherResourcePtr searcherResource =
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
                    searcherQueryResource->getCavaJitModules());
        OP_REQUIRES(ctx, commonResource,
                    errors::Unavailable("create common resource failed"));

        auto partitionReaderSnapshot = searcherQueryResource->getIndexSnapshotPtr();
        OP_REQUIRES(ctx, partitionReaderSnapshot,
                    errors::Unavailable("partitionReaderSnapshot unavailable"));
        IndexPartitionReaderWrapperPtr idxPartReaderWrapperPtr =
            searcherResource->createPartialIndexPartitionReaderWrapper(
                    partitionReaderSnapshot);
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
                partitionResource->attributeExpressionCreator.get());
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

END_HA3_NAMESPACE(turing);
