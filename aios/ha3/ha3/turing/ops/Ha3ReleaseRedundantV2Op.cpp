#include <ha3/turing/ops/Ha3ReleaseRedundantV2Op.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;

USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
BEGIN_HA3_NAMESPACE(turing);
USE_HA3_NAMESPACE(func_expression);

void Ha3ReleaseRedundantV2Op::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    OP_REQUIRES(ctx, sessionResource,
                errors::Unavailable("session resource unavailable"));

    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    OP_REQUIRES(ctx, searcherQueryResource,
                errors::Unavailable("ha3 searcher query resource unavailable"));

    SearchRuntimeResourcePtr runtimeResource = searcherQueryResource->runtimeResource;
    OP_REQUIRES(ctx, runtimeResource,
                errors::Unavailable("ha3 runtime resource unavailable"));

    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto requestVariant = requestTensor.get<Ha3RequestVariant>();
    RequestPtr request = requestVariant->getRequest();

    auto requiredTopK = runtimeResource->docCountLimits.requiredTopK;
    auto matchdocsTensor = ctx->input(1).scalar<Variant>()();
    auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();
    common::Ha3MatchDocAllocatorPtr ha3MatchDocAllocator =
        dynamic_pointer_cast<common::Ha3MatchDocAllocator>(matchDocsVariant->getAllocator());
    OP_REQUIRES(ctx, ha3MatchDocAllocator,
                errors::Unavailable("dynamic_pointer_cast matchDocAllocator failed."));

    vector<matchdoc::MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);

    uint32_t resultCount = min((uint32_t)matchDocVec.size(), getResultCount(request.get(), requiredTopK, searcherQueryResource));
    releaseRedundantMatchDocs(matchDocVec, resultCount, ha3MatchDocAllocator);
    matchDocsVariant->stealMatchDocs(matchDocVec);
    Tensor* matchDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocs));
    matchDocs->scalar<Variant>()() = *matchDocsVariant;
}

void Ha3ReleaseRedundantV2Op::releaseRedundantMatchDocs(std::vector<matchdoc::MatchDoc> &matchDocVec,
                                   uint32_t resultCount,
                                   const common::Ha3MatchDocAllocatorPtr &matchDocAllocator)
{
    uint32_t size = (uint32_t)matchDocVec.size();
    while(size > resultCount)
    {
        auto matchDoc = matchDocVec[--size];
        matchDocAllocator->deallocate(matchDoc);
    }
    matchDocVec.erase(matchDocVec.begin() + size, matchDocVec.end());
}

uint32_t Ha3ReleaseRedundantV2Op::getResultCount(const Request *request, uint32_t requiredTopK, SearcherQueryResource *searcherQueryResource)
{
    SearcherCacheInfoPtr searchCacheInfo = searcherQueryResource->searcherCacheInfo;
    SearcherCacheClause *cacheClause = request->getSearcherCacheClause();
    if (!searchCacheInfo || searchCacheInfo->isHit || !cacheClause) {
        return requiredTopK;
    }

    const vector<uint32_t> &cacheDocNumVec = cacheClause->getCacheDocNumVec();
    uint32_t size = cacheDocNumVec.size();
    assert(size > 0);
    for (size_t i = 0; i < size; ++i) {
        if (cacheDocNumVec[i] >= requiredTopK) {
            return cacheDocNumVec[i];
        }
    }
    return requiredTopK;
}

HA3_LOG_SETUP(turing, Ha3ReleaseRedundantV2Op);

REGISTER_KERNEL_BUILDER(Name("Ha3ReleaseRedundantV2Op")
                        .Device(DEVICE_CPU),
                        Ha3ReleaseRedundantV2Op);

END_HA3_NAMESPACE(turing);
