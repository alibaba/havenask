#include <ha3/common.h>
#include <ha3/isearch.h>
#include <ha3/util/Log.h>
#include <tensorflow/core/framework/op_kernel.h>
#include <ha3/turing/variant/ExpressionResourceVariant.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/ops/agg/PrepareExpressionResourceOp.h>
#include <ha3/common/Request.h>
#include <ha3/common/ha3_op_common.h>
#include <basic_ops/util/OpUtil.h>
#include <suez/turing/expression/framework/AttributeExpressionCreator.h>
#include <suez/turing/expression/provider/FunctionProvider.h>
#include <matchdoc/MatchDocAllocator.h>

using namespace tensorflow;
using namespace matchdoc;
using namespace suez::turing;
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, PrepareExpressionResourceOp);


void PrepareExpressionResourceOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    auto requestVariant = ctx->input(0).scalar<Variant>()().get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, requestVariant, errors::Unavailable("get ha3 request variant failed"));
    RequestPtr request = requestVariant->getRequest();
    OP_REQUIRES(ctx, request, errors::Unavailable("get ha3 request failed"));
    auto tableInfoPtr = sessionResource->tableInfo;
    OP_REQUIRES(ctx, tableInfoPtr, errors::Unavailable("get table info failed"));

    ExpressionResourcePtr resource = createExpressionResource(request, tableInfoPtr,
            sessionResource->bizInfo._itemTableName, sessionResource->functionInterfaceCreator,
            queryResource->getIndexSnapshot(), queryResource->getTracer(),
            sessionResource->cavaPluginManager, queryResource->getPool(),
            queryResource->getCavaAllocator());
    OP_REQUIRES(ctx, resource, errors::Unavailable("create expression resource failed"));

    ExpressionResourceVariant resourceVariant(resource);
    Tensor *outputTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
    outputTensor->scalar<Variant>()() = resourceVariant;
}

ExpressionResourcePtr PrepareExpressionResourceOp::createExpressionResource(
        const RequestPtr &request,
        const TableInfoPtr &tableInfoPtr,
        const string &mainTableName,
        const FunctionInterfaceCreatorPtr &functionInterfaceCreator,
        IE_NAMESPACE(partition)::PartitionReaderSnapshot *partitionReaderSnapshot,
        Tracer *tracer,
        const suez::turing::CavaPluginManagerPtr &cavaPluginManager,
        autil::mem_pool::Pool *pool,
        suez::turing::SuezCavaAllocator *cavaAllocator)
{
    common::ConfigClause *configClause = request->getConfigClause();
    if (configClause == NULL) {
        HA3_LOG(WARN, "config clause is empty.");
        return ExpressionResourcePtr();
    }
    bool useSub = useSubDoc(request.get(), tableInfoPtr.get());
    MatchDocAllocatorPtr matchDocAllocator(new MatchDocAllocator(pool, useSub));
    FunctionProviderPtr functionProvider;
    functionProvider.reset(new FunctionProvider(matchDocAllocator.get(),
                    pool, cavaAllocator, tracer, partitionReaderSnapshot,
                    configClause->getKeyValueMapPtr().get()));

    std::vector<suez::turing::VirtualAttribute*> virtualAttributes;
    auto virtualAttributeClause = request->getVirtualAttributeClause();
    if (virtualAttributeClause) {
        virtualAttributes = virtualAttributeClause->getVirtualAttributes();
    }
    AttributeExpressionCreatorPtr attributeExpressionCreator;
    attributeExpressionCreator.reset(new AttributeExpressionCreator(
                    pool, matchDocAllocator.get(),
                    mainTableName, partitionReaderSnapshot,
                    tableInfoPtr, virtualAttributes,
                    functionInterfaceCreator.get(),
                    cavaPluginManager, functionProvider.get()));

    ExpressionResourcePtr resource(new ExpressionResource(request, matchDocAllocator,
                    functionProvider, attributeExpressionCreator));
    return resource;
}

bool PrepareExpressionResourceOp::useSubDoc(const Request *request, const TableInfo *tableInfo) {
    if (!request) {
        return false;
    }
    common::ConfigClause *configClause = request->getConfigClause();
    if (configClause != NULL
        && configClause->getSubDocDisplayType() != SUB_DOC_DISPLAY_NO
        && tableInfo && tableInfo->hasSubSchema())
    {
        return true;
    }
    return false;
}

REGISTER_KERNEL_BUILDER(Name("PrepareExpressionResourceOp")
                        .Device(DEVICE_CPU),
                        PrepareExpressionResourceOp);

END_HA3_NAMESPACE(turing);
