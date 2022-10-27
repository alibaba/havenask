#include <ha3/turing/ops/agg/LayerMetasCreateOp.h>
#include <ha3/turing/ops/SearcherQueryResource.h>
#include <ha3/search/IndexPartitionReaderUtil.h>
#include <ha3/search/LayerMetasCreator.h>

using namespace tensorflow;
using namespace suez::turing;
using namespace std;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(search);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, LayerMetasCreateOp);

void LayerMetasCreateOp::Compute(tensorflow::OpKernelContext* ctx) {
    // get search query resource
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);

    auto requestVariant = ctx->input(0).scalar<Variant>()().get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, requestVariant, errors::Unavailable("get ha3 request variant failed"));
    RequestPtr request = requestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("get ha3 request failed"));

    string mainTableName = sessionResource->bizInfo._itemTableName;
    LayerMetasPtr layerMetas = createLayerMetas(queryResource->getIndexSnapshot(),
            mainTableName, queryResource->getPool(), request->getQueryLayerClause());
    OP_REQUIRES(ctx, layerMetas, errors::Unavailable("create layer metas failed"));
    updateQuota(layerMetas);
    LayerMetasVariant layerMetasVariant(layerMetas);
    Tensor *layerMetaTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &layerMetaTensor));
    layerMetaTensor->scalar<Variant>()() = layerMetasVariant;
}

LayerMetasPtr LayerMetasCreateOp::createLayerMetas(
        IE_NAMESPACE(partition)::PartitionReaderSnapshot *indexSnapshot,
        const std::string &mainTableName,
        autil::mem_pool::Pool *pool,
        QueryLayerClause* queryLayerClause)
{
    IndexPartitionReaderWrapperPtr readerWrapper =
        IndexPartitionReaderUtil::createIndexPartitionReaderWrapper(indexSnapshot,
                mainTableName);
    if (readerWrapper == nullptr) {
        HA3_LOG(WARN, "create index partition reader wrapper failed.");
        return LayerMetasPtr();
    }
    readerWrapper->setSessionPool(pool);
    LayerMetasCreator layerMetasCreator;
    layerMetasCreator.init(pool, readerWrapper.get());
    LayerMetasPtr layerMetas(layerMetasCreator.create(queryLayerClause),
                                [](LayerMetas* p) {
                POOL_DELETE_CLASS(p);
            });
    return layerMetas;
}

void LayerMetasCreateOp::updateQuota(LayerMetasPtr &layerMetas) {
    if (layerMetas == NULL) {
        return;
    }
    for (auto &layerMeta : *layerMetas) {
        for (auto &rangeMeta : layerMeta) {
            rangeMeta.quota =  rangeMeta.end - rangeMeta.begin + 1;
        }
    }
}

REGISTER_KERNEL_BUILDER(Name("LayerMetasCreateOp")
                        .Device(DEVICE_CPU),
                        LayerMetasCreateOp);

END_HA3_NAMESPACE(turing);
