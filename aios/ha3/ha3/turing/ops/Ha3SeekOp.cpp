#include <ha3/turing/ops/Ha3SeekOp.h>
#include <ha3/search/InnerSearchResult.h>
#include <ha3/service/SearcherResource.h>
#include <indexlib/partition/index_partition.h>
#include <ha3/monitor/SessionMetricsCollector.h>
#include <ha3/turing/variant/Ha3RequestVariant.h>
#include <ha3/turing/variant/Ha3ResultVariant.h>
#include <ha3/turing/variant/AggregateResultsVariant.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(func_expression);
BEGIN_HA3_NAMESPACE(turing);

HA3_LOG_SETUP(turing, Ha3SeekOp);

void Ha3SeekOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    SearcherSessionResource *searcherSessionResource = dynamic_cast<SearcherSessionResource *>(sessionResource);
    SearcherQueryResource *searcherQueryResource =  dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherSessionResource,
                      errors::Unavailable("ha3 searcher session resource unavailable"));
    OP_REQUIRES(ctx, searcherQueryResource,
                      errors::Unavailable("ha3 searcher query resource unavailable"));
    HA3_NS(service)::SearcherResource *searcherResource =
        searcherSessionResource->searcherResource.get();
    OP_REQUIRES(ctx, searcherResource,
                      errors::Unavailable("ha3 searcher resource unavailable"));
    auto pool = searcherQueryResource->getPool();
    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto ha3RequestVariant = requestTensor.get<Ha3RequestVariant>();
    Request *request = ha3RequestVariant->getRequest().get();
    OP_REQUIRES(ctx, request, errors::Unavailable("ha3 request unavailable"));

    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    const rank::RankProfile *rankProfile = nullptr;
    if (!rankProfileMgr->getRankProfile(request,
                    rankProfile, searcherQueryResource->commonResource->errorResult))
    {
        OP_REQUIRES(ctx, false, errors::Unavailable("get rank profile failed."));
    }
    seek(ctx, request, rankProfile, pool, searcherSessionResource,
         searcherQueryResource,  searcherResource);
}

void Ha3SeekOp::seek(tensorflow::OpKernelContext* ctx,
                     common::Request *request,
                     const rank::RankProfile *rankProfile,
                     autil::mem_pool::Pool *pool,
                     SearcherSessionResource *searcherSessionResource,
                     SearcherQueryResource *searcherQueryResource,
                     service::SearcherResource *searcherResource)
{
    search::InnerSearchResult innerResult(pool);
    bool ret = processRequest(request, rankProfile, searcherQueryResource, searcherResource, innerResult);
    outputResult(ctx, ret, innerResult, request, pool, searcherSessionResource, searcherQueryResource, searcherResource);
}

void Ha3SeekOp::outputResult(tensorflow::OpKernelContext* ctx,
                             bool ret,
                             search::InnerSearchResult &innerResult,
                             common::Request *request,
                             autil::mem_pool::Pool *pool,
                             SearcherSessionResource *searcherSessionResource,
                             SearcherQueryResource *searcherQueryResource,
                             service::SearcherResource *searcherResource)
{
    if (!ret) {
        if (searcherQueryResource) {
            auto commonResource = searcherQueryResource->commonResource.get();
            if (commonResource) {
                common::ResultPtr errorResult = constructErrorResult(request, searcherSessionResource,
                        searcherQueryResource, searcherResource, commonResource);
                Ha3ResultVariant ha3ErrorResultVariant(errorResult, pool);
                tensorflow::Tensor* errorResultTensor = nullptr;
                OP_REQUIRES_OK(ctx, ctx->allocate_output(5, {}, &errorResultTensor));
                errorResultTensor->scalar<tensorflow::Variant>()() = ha3ErrorResultVariant;
                return;
            }
        }
        OP_REQUIRES(ctx, false, tensorflow::errors::Unavailable("construct error result failed."));
    }
    common::Ha3MatchDocAllocatorPtr ha3Allocator = innerResult.matchDocAllocatorPtr;
    matchdoc::MatchDocAllocatorPtr allocator =
        std::dynamic_pointer_cast<matchdoc::MatchDocAllocator>(ha3Allocator);
    OP_REQUIRES(ctx, allocator,
                      tensorflow::errors::Unavailable("dynamic_pointer_cast ha3Allocator failed."));

    suez::turing::MatchDocsVariant matchDocsVariant(allocator, pool);
    std::vector<matchdoc::MatchDoc> matchDocVec;
    matchDocVec.insert(matchDocVec.begin(),
                       innerResult.matchDocVec.begin(), innerResult.matchDocVec.end());
    matchDocsVariant.stealMatchDocs(matchDocVec);

    tensorflow::Tensor* matchDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocs));
    matchDocs->scalar<tensorflow::Variant>()() = matchDocsVariant;

    tensorflow::Tensor* extraDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &extraDocs));
    extraDocs->scalar<uint32_t>()() = innerResult.extraRankCount;

    tensorflow::Tensor* totalDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &totalDocs));
    totalDocs->scalar<uint32_t>()() = innerResult.totalMatchDocs;

    tensorflow::Tensor* actualDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(3, {}, &actualDocs));
    actualDocs->scalar<uint32_t>()() = innerResult.actualMatchDocs;


    AggregateResultsVariant ha3AggResultsVariant(innerResult.aggResultsPtr,  pool);
    tensorflow::Tensor* aggResults = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(4, {}, &aggResults));
    aggResults->scalar<tensorflow::Variant>()() = ha3AggResultsVariant;

}

bool Ha3SeekOp::processRequest(common::Request *request,
                               const rank::RankProfile *rankProfile,
                               SearcherQueryResource *searcherQueryResource,
                               service::SearcherResource *searcherResource,
                               search::InnerSearchResult &innerResult)
{
    auto &commonResource = searcherQueryResource->commonResource;
    auto &partitionResource = searcherQueryResource->partitionResource;
    auto &runtimeResource = searcherQueryResource->runtimeResource;
    if (!commonResource || !partitionResource || !runtimeResource) {
        return false;
    }
    auto rankProfileMgr = searcherResource->getRankProfileManager().get();
    auto optimizerChainManager = searcherResource->getOptimizerChainManager().get();
    auto sorterManager = searcherResource->getSorterManager().get();
    auto searcherCache = searcherResource->getSearcherCache().get();
    search::MatchDocSearcher searcher(*commonResource, *partitionResource, *runtimeResource,
            rankProfileMgr, optimizerChainManager, sorterManager,
            searcherResource->getAggSamplerConfigInfo(), searcherResource->getClusterConfig(),
            searcherResource->getPartCount(), searcherCache, rankProfile);

    search::SearcherCacheInfoPtr searcherCacheInfo = searcherQueryResource->searcherCacheInfo;
    return searcher.seek(request,searcherCacheInfo.get(), innerResult);
}

common::ResultPtr Ha3SeekOp::constructErrorResult(common::Request *request,
        SearcherSessionResource *searcherSessionResource,
        SearcherQueryResource *searcherQueryResource,
        service::SearcherResource *searcherResource,
        search::SearchCommonResource *commonResource)
{
    common::MatchDocs *matchDocs = new common::MatchDocs();
    common::ResultPtr result = common::ResultPtr(new common::Result(matchDocs));
    result->addErrorResult(commonResource->errorResult);
    auto tracer = searcherQueryResource->getTracerPtr();
    result->setTracer(tracer);

    if (request && searcherResource && searcherSessionResource && searcherQueryResource) {
        uint32_t ip = searcherSessionResource->getIp();
        auto idxPartReaderWrapperPtr
            = searcherQueryResource->indexPartitionReaderWrapper;
        auto phaseOneInfoMask = request->getConfigClause()->getPhaseOneBasicInfoMask();
        versionid_t versionId = idxPartReaderWrapperPtr->getCurrentVersion();
        matchDocs->setGlobalIdInfo(searcherResource->getHashIdRange().from(),
                versionId, searcherResource->getFullIndexVersion(), ip, phaseOneInfoMask);
        const proto::PartitionID &partitionId = searcherResource->getPartitionId();
        result->addCoveredRange(partitionId.clustername(),
                                partitionId.range().from(), partitionId.range().to());
    }
    return result;
}

REGISTER_KERNEL_BUILDER(Name("Ha3SeekOp")
                        .Device(DEVICE_CPU),
                        Ha3SeekOp);

END_HA3_NAMESPACE(turing);
