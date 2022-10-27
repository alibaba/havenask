#include "ha3/turing/ops/Ha3SeekParaOp.h"

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(func_expression);


void Ha3SeekParaOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    SearcherSessionResource *searcherSessionResource = dynamic_cast<SearcherSessionResource *>(sessionResource);
    OP_REQUIRES(ctx, searcherSessionResource,
                      errors::Unavailable("ha3 searcher session resource unavailable"));
    HA3_NS(service)::SearcherResource *searcherResource =
        searcherSessionResource->searcherResource.get();
    OP_REQUIRES(ctx, searcherResource,
                      errors::Unavailable("ha3 searcher resource unavailable"));
    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, ha3RequestVariant,
                      errors::Unavailable("ha3 request variant unavailable"));
    Request *request = ha3RequestVariant->getRequest().get();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));
    auto seekResourceVariant = ctx->input(1).scalar<Variant>()().get<SeekResourceVariant>();
    OP_REQUIRES(ctx, seekResourceVariant,
                      errors::Unavailable("seek resource variant unavailable"));
    SeekResource *seekResource = seekResourceVariant->getSeekResource().get();
    OP_REQUIRES(ctx, seekResource, errors::Unavailable("seek resource unavailable"));
    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    const rank::RankProfile *rankProfile = nullptr;
    if (!rankProfileMgr->getRankProfile(request,
                    rankProfile, seekResource->commonResource->errorResult))
    {
        OP_REQUIRES(ctx, false, errors::Unavailable("get rank profile failed."));
    }
    seek(ctx, request, rankProfile, seekResource,  searcherResource);
}

void Ha3SeekParaOp::seek(tensorflow::OpKernelContext* ctx,
                         common::Request *request,
                         const rank::RankProfile *rankProfile,
                         SeekResource *seekResource,
                         service::SearcherResource *searcherResource)
{
    search::InnerSearchResult innerResult(seekResource->commonResource->pool);
    bool ret = processRequest(request, rankProfile, seekResource, searcherResource, innerResult);
    REQUEST_TRACE_WITH_TRACER(seekResource->commonResource->tracer, TRACE3,
                              "===== end parallel seek =====");
    tensorflow::Tensor* innerResultTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &innerResultTensor));
    InnerResultVariant innerResultVariant(innerResult);
    innerResultTensor->scalar<tensorflow::Variant>()() = innerResultVariant;
    tensorflow::Tensor* hasErrorTensor = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &hasErrorTensor));
    hasErrorTensor->scalar<bool>()() = !ret;
}

bool Ha3SeekParaOp::processRequest(common::Request *request,
                                   const rank::RankProfile *rankProfile,
                                   SeekResource *seekResource,
                                   service::SearcherResource *searcherResource,
                                   search::InnerSearchResult &innerResult)
{
    auto &commonResource = seekResource->commonResource;
    auto &partitionResource = seekResource->partitionResource;
    auto &runtimeResource = seekResource->runtimeResource;
    auto &layerMetas = seekResource->layerMetas;
    if (!commonResource || !partitionResource || !runtimeResource) {
        return false;
    }
    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    auto optimizerChainManager = searcherResource->getOptimizerChainManager().get();
    auto sorterManager = searcherResource->getSorterManager().get();
    auto searcherCache = searcherResource->getSearcherCache().get();
    search::MatchDocSearcher searcher(*commonResource, *partitionResource,
            *runtimeResource, rankProfileMgr,
            optimizerChainManager, sorterManager,
            searcherResource->getAggSamplerConfigInfo(),
            searcherResource->getClusterConfig(),
            searcherResource->getPartCount(), searcherCache, rankProfile,
            layerMetas);
    return searcher.seek(request, nullptr, innerResult);
}

HA3_LOG_SETUP(turing, Ha3SeekParaOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SeekParaOp")
                        .Device(DEVICE_CPU),
                        Ha3SeekParaOp);

END_HA3_NAMESPACE(turing);
