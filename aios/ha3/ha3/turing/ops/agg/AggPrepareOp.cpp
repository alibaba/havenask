#include <ha3/turing/ops/agg/AggPrepareOp.h>
#include <ha3/turing/variant/AggregatorVariant.h>
#include <ha3/search/AggregatorCreator.h>
#include <ha3/search/Aggregator.h>
#include <ha3/common/Request.h>

using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(common);

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, AggPrepareOp);

AggPrepareOp::AggPrepareOp(tensorflow::OpKernelConstruction* ctx)
    : tensorflow::OpKernel(ctx)
{     
    auto sessionResource = GET_SESSION_RESOURCE();
    auto resourceReader = sessionResource->resourceReader;
    OP_REQUIRES(ctx, resourceReader, errors::Internal("resource reader is null."));
    HA3_LOG(INFO, "get config root path[%s]", resourceReader->getConfigPath().c_str());

    std::string relativePath, jsonPath;
    OP_REQUIRES_OK(ctx, ctx->GetAttr("relative_path", &relativePath));
    OP_REQUIRES_OK(ctx, ctx->GetAttr("json_path", &jsonPath));
    if (!relativePath.empty() && !jsonPath.empty()) {
        bool ret = resourceReader->getConfigWithJsonPath(relativePath, jsonPath, _aggSamplerConfig);
        if (!ret) {
            HA3_LOG(WARN, "get agg sample config  fail, file [%s], path [%s]", relativePath.c_str(),
                    jsonPath.c_str());
        } else {
            if (_aggSamplerConfig.getAggBatchMode()) {
                _aggSamplerConfig.setAggBatchMode(false);
            }
        }
    }
}

void AggPrepareOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    
    auto resourceVariant = ctx->input(0).scalar<Variant>()().get<ExpressionResourceVariant>();
    OP_REQUIRES(ctx, resourceVariant, errors::Unavailable("get expression resource variant failed"));
    ExpressionResourcePtr resource = resourceVariant->getExpressionResource();
    OP_REQUIRES(ctx, resource, errors::Unavailable("expression resource is empty."));

    AggregatorPtr aggregator = createAggregator(resource, queryResource, _aggSamplerConfig);
    OP_REQUIRES(ctx, aggregator, errors::Unavailable("prepare aggregator failed."));

    AggregatorVariant aggregatorVariant(aggregator, resource);
    Tensor *outputTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, TensorShape({}), &outputTensor));
    outputTensor->scalar<Variant>()() = aggregatorVariant;
}

AggregatorPtr AggPrepareOp::createAggregator(const ExpressionResourcePtr &resource,
        tensorflow::QueryResource *queryResource,
        const config::AggSamplerConfigInfo &configInfo)
{
    config::AggSamplerConfigInfo aggSamplerConfigInfo = configInfo;
    autil::mem_pool::Pool *pool = queryResource->getPool();
    auto configClause = resource->_request->getConfigClause();
    if (configClause) {
        const auto &enableAggJit = configClause->getKVPairValue("enableAggJit");
        if (!enableAggJit.empty()) {
            bool enableOrNot = enableAggJit == "true" ? true : false;
            aggSamplerConfigInfo.setEnableJit(enableOrNot);
        }
    }
    AggregatorCreator aggCreator(resource->_attributeExpressionCreator.get(), pool, queryResource);
    aggCreator.setAggSamplerConfigInfo(aggSamplerConfigInfo);
    AggregatorPtr aggregator(aggCreator.createAggregator(
                    resource->_request->getAggregateClause()));
    if (aggregator == nullptr) {
        HA3_LOG(WARN, "create aggregator failed.");        
        return aggregator;
    }
    matchdoc::MatchDocAllocatorPtr matchDocAllocator = resource->_matchDocAllocator;
    aggregator->setMatchDocAllocator(matchDocAllocator.get());
    return aggregator;
}

REGISTER_KERNEL_BUILDER(Name("AggPrepareOp")
                        .Device(DEVICE_CPU),
                        AggPrepareOp);

END_HA3_NAMESPACE(turing);
