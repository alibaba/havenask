#include <ha3/turing/ops/Ha3SeekParaSplitOp.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/search/LayerMetasCreator.h>
#include <ha3/search/LayerMetaUtil.h>

using namespace autil::mem_pool;
using namespace tensorflow;
using namespace suez::turing;
using namespace std;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, Ha3SeekParaSplitOp);

void Ha3SeekParaSplitOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    auto requestVariant =
        ctx->input(0).scalar<Variant>()().get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, requestVariant,
                errors::Unavailable("get ha3 request variant failed"));
    RequestPtr request = requestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("get ha3 request failed"));
    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherQueryResource,
                errors::Unavailable("get searcher query resource failed"));
    const LayerMetasPtr &layerMetas = LayerMetaUtil::createLayerMetas(
            request->getQueryLayerClause(),
            request->getRankClause(),
            searcherQueryResource->indexPartitionReaderWrapper.get(),
            searcherQueryResource->getPool());
    OP_REQUIRES(ctx, layerMetas, errors::Unavailable("create layer metas failed"));

    std::vector<LayerMetasPtr> splitedLayerMetas;
    auto readerWrapper = searcherQueryResource->indexPartitionReaderWrapper;
    bool ret = LayerMetaUtil::splitLayerMetas(searcherQueryResource->getPool(),
            *layerMetas, readerWrapper, _outputNum, splitedLayerMetas);
    OP_REQUIRES(ctx, ret, errors::Unavailable("split layer metas failed"));

    for (int idx = 0; idx < _outputNum; idx++) {
        LayerMetasVariant outVariant(splitedLayerMetas[idx]);
        Tensor *layerMetaTensor = nullptr;
        OP_REQUIRES_OK(ctx, ctx->allocate_output(
                        idx, TensorShape({}), &layerMetaTensor));
        layerMetaTensor->scalar<Variant>()() = outVariant;
    }

    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
    if (metricsCollector) {
        metricsCollector->parallelSeekStartTrigger();
    }
}

REGISTER_KERNEL_BUILDER(Name("Ha3SeekParaSplitOp")
                        .Device(DEVICE_CPU),
                        Ha3SeekParaSplitOp);

END_HA3_NAMESPACE(turing);
