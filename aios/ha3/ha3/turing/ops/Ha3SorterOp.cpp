#include <ha3/turing/ops/Ha3SorterOp.h>

using namespace std;
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(sorter);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(func_expression);

BEGIN_HA3_NAMESPACE(turing);

void Ha3SorterOp::Compute(tensorflow::OpKernelContext* ctx) {
    auto sessionResource = GET_SESSION_RESOURCE();
    auto queryResource = GET_QUERY_RESOURCE(sessionResource);
    CHECK_TIMEOUT(queryResource);
    REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(), TRACE3, "begin ha3 sorter op");
    auto requestTensor = ctx->input(0).scalar<Variant>()();
    auto requestVariant = requestTensor.get<Ha3RequestVariant>();
    OP_REQUIRES(ctx, requestVariant, errors::Unavailable("get requestVariant failed"));
    RequestPtr request = requestVariant->getRequest();
    auto matchdocsTensor = ctx->input(1).scalar<Variant>()();
    auto matchDocsVariant = matchdocsTensor.get<MatchDocsVariant>();
    OP_REQUIRES(ctx, matchDocsVariant, errors::Unavailable("get matchDocsVariant failed"));
    uint32_t extraRankCount = ctx->input(2).scalar<uint32_t>()();
    uint32_t totalMatchDocs = ctx->input(3).scalar<uint32_t>()();
    uint32_t actualMatchDocs = ctx->input(4).scalar<uint32_t>()();
    uint32_t srcCount = (uint32_t)ctx->input(5).scalar<uint16_t>()();
    bool ret = false;
    if (_location == SL_SEARCHER) {
        ret = sortInSearcher(request, sessionResource, queryResource, matchDocsVariant,
                             extraRankCount, totalMatchDocs, actualMatchDocs);
    } else if (_location == HA3_NS(sorter)::SL_QRS || _location == SL_SEARCHCACHEMERGER) {
        ret = sortInQrs(request, sessionResource, queryResource, matchDocsVariant,
                        extraRankCount, totalMatchDocs, actualMatchDocs, srcCount);
    }
    OP_REQUIRES(ctx, ret, errors::Unavailable("sort fail"));
    REQUEST_TRACE_WITH_TRACER(queryResource->getTracer(), TRACE3, "end ha3 sorter op");

    Tensor* matchDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(0, {}, &matchDocs));
    matchDocs->scalar<Variant>()() = *matchDocsVariant;

    Tensor* totalDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(1, {}, &totalDocs));
    totalDocs->scalar<uint32_t>()() = totalMatchDocs;

    Tensor* actualDocs = nullptr;
    OP_REQUIRES_OK(ctx, ctx->allocate_output(2, {}, &actualDocs));
    actualDocs->scalar<uint32_t>()() = actualMatchDocs;
}


bool Ha3SorterOp::sortInSearcher(RequestPtr &request,
                                 SessionResource *sessionResource,
                                 QueryResource *queryResource,
                                 MatchDocsVariant *matchDocsVariant,
                                 uint32_t extraRankCount,
                                 uint32_t &totalMatchDocs,
                                 uint32_t &actualMatchDocs)
{
    SearcherSessionResource *searcherSessionResource =
        dynamic_cast<SearcherSessionResource *>(sessionResource);
    if (!searcherSessionResource) {
        HA3_LOG(WARN, "ha3 searcher session resource unavailable");
        return false;
    }
    SearcherQueryResource *searcherQueryResource =
        dynamic_cast<SearcherQueryResource *>(queryResource);
    if (!searcherQueryResource) {
        HA3_LOG(WARN, "ha3 searcher query resource unavailable");
        return false;
    }
    auto metricsCollector = searcherQueryResource->sessionMetricsCollector;
    if (!metricsCollector) {
        HA3_LOG(WARN, "metrics collector is null.");
        return false;
    }
    SearchCommonResourcePtr commonResource = searcherQueryResource->commonResource;
    if (!commonResource) {
        HA3_LOG(WARN, "get common resource failed.");
        return false;
    }
    metricsCollector->extraRankStartTrigger();

    HA3_NS(service)::SearcherResourcePtr searcherResource =
        searcherSessionResource->searcherResource;
    if (!searcherResource) {
        HA3_LOG(WARN, "ha3 searcher resource unavailable");
        return false;
    }
    auto pool = searcherQueryResource->getPool();
    IndexPartitionReaderWrapperPtr idxPartReaderWrapperPtr
        = searcherQueryResource->indexPartitionReaderWrapper;
    if (idxPartReaderWrapperPtr == NULL) {
        HA3_LOG(WARN, "ha3 index partition reader wrapper unavailable");
        return false;
    }
    common::Ha3MatchDocAllocatorPtr ha3MatchDocAllocator =
        dynamic_pointer_cast<common::Ha3MatchDocAllocator>(matchDocsVariant->getAllocator());
    if(!ha3MatchDocAllocator) {
        HA3_LOG(WARN, "dynamic_pointer_cast matchDocAllocator failed.");
        return false;
    }
    SearchPartitionResourcePtr partitionResource = searcherQueryResource->partitionResource;
    if (!partitionResource) {
        HA3_LOG(WARN, "get partition resource failed.");
        return false;
    }
    SearchRuntimeResourcePtr runtimeResource = searcherQueryResource->runtimeResource;
    if (!runtimeResource) {
        HA3_LOG(WARN, "get runtime resource failed.");
        return false;
    }
    sorter::SorterResource sorterResource = createSorterResource(request.get(),
            *commonResource, *partitionResource, *runtimeResource);

    SorterWrapperPtr finalSorterWrapper(createFinalSorterWrapper(request.get(),
                    searcherResource->getSorterManager().get(), pool));
    if (!finalSorterWrapper) {
        HA3_LOG(WARN, "The final sorter is not exist!");
        return false;
    }
    sorter::SorterProvider sorterProvider(sorterResource);
    if (!finalSorterWrapper->beginSort(&sorterProvider)) {
        std::string errorMsg = "Sorter begin request failed : [" +
                               sorterProvider.getErrorMessage() + "]";
        HA3_LOG(WARN, "%s", errorMsg.c_str());
        commonResource->errorResult->addError(ERROR_SORTER_BEGIN_REQUEST, errorMsg);
        return false;
    }

    ha3MatchDocAllocator->extend();
    if (ha3MatchDocAllocator->hasSubDocAllocator()) {
        ha3MatchDocAllocator->extendSub();
    }
    std::vector<matchdoc::MatchDoc> matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);

    if (matchDocVec.size() == 0) {
        metricsCollector->extraRankCountTrigger(0);
        metricsCollector->extraRankEndTrigger();
        return true;
    }

    SortParam sortParam(pool);
    sortParam.requiredTopK = extraRankCount;
    sortParam.totalMatchCount = totalMatchDocs;
    sortParam.actualMatchCount = actualMatchDocs;
    metricsCollector->extraRankCountTrigger(matchDocVec.size());
    sortParam.matchDocs.insert(sortParam.matchDocs.begin(),
                               matchDocVec.begin(), matchDocVec.end());
    finalSorterWrapper->sort(sortParam);
    finalSorterWrapper->endSort();
    doLazyEvaluate(sortParam.matchDocs, ha3MatchDocAllocator,
                   runtimeResource->comparatorCreator.get());
    matchDocVec.clear();
    matchDocVec.insert(matchDocVec.begin(), sortParam.matchDocs.begin(),
                       sortParam.matchDocs.end());
    matchDocsVariant->stealMatchDocs(matchDocVec);
    totalMatchDocs = sortParam.totalMatchCount;
    actualMatchDocs = sortParam.actualMatchCount;
    metricsCollector->extraRankEndTrigger();
    return true;
}

void Ha3SorterOp::doLazyEvaluate(autil::mem_pool::PoolVector<matchdoc::MatchDoc> &matchDocVect,
                    common::Ha3MatchDocAllocatorPtr &matchDocAllocator,
                    const rank::ComparatorCreator *comparatorCreator) {
    const ExpressionSet &exprSet = comparatorCreator->getLazyEvaluateExpressions();
    ExpressionEvaluator<ExpressionSet> evaluator(exprSet,
            matchDocAllocator->getSubDocAccessor());
    evaluator.batchEvaluateExpressions(&matchDocVect[0], matchDocVect.size());
}


SorterWrapper *Ha3SorterOp::createFinalSorterWrapper(const Request *request,
                                        const SorterManager *sorterManager,
                                        autil::mem_pool::Pool *pool) const
{
    std::string sorterName = "DEFAULT";
    auto finalSortClause = request->getFinalSortClause();
    if (finalSortClause) {
        sorterName = finalSortClause->getSortName();
    }
    auto sorter = sorterManager->getSorter(sorterName);
    if (!sorter) {
        return NULL;
    }
    return new SorterWrapper(sorter->clone());
}

sorter::SorterResource Ha3SorterOp::createSorterResource(const common::Request *request,
        SearchCommonResource &commonResource,
        SearchPartitionResource &partitionResource,
        SearchRuntimeResource &runtimeResource)
{
    sorter::SorterResource sorterResource;

    sorterResource.location = _location;
    auto scoreRef = commonResource.matchDocAllocator->findReference<score_t>(SCORE_REF);
    sorterResource.scoreExpression = NULL;
    if (scoreRef) {
        sorterResource.scoreExpression =
            AttributeExpressionFactory::createAttributeExpression(scoreRef, commonResource.pool);
    }
    sorterResource.requestTracer = commonResource.tracer;
    sorterResource.pool = commonResource.pool;
    sorterResource.request = request;

    int32_t totalDocCount = partitionResource.getTotalDocCount();
    sorterResource.globalMatchData.setDocCount(totalDocCount);
    sorterResource.matchDataManager = &partitionResource.matchDataManager;
    sorterResource.attrExprCreator = partitionResource.attributeExpressionCreator.get();
    sorterResource.dataProvider = &commonResource.dataProvider;
    sorterResource.matchDocAllocator = commonResource.matchDocAllocator;
    sorterResource.fieldInfos = commonResource.tableInfo->getFieldInfos();
    // support sorter update requiredTopk!!.
    sorterResource.requiredTopk = &runtimeResource.docCountLimits.requiredTopK;
    sorterResource.comparatorCreator = runtimeResource.comparatorCreator.get();
    sorterResource.sortExprCreator = runtimeResource.sortExpressionCreator.get();
    sorterResource.queryTerminator = commonResource.timeoutTerminator;
    sorterResource.kvpairs = &request->getConfigClause()->getKVPairs();
    sorterResource.partitionReaderSnapshot = partitionResource.partitionReaderSnapshot.get();
    return sorterResource;
}

bool Ha3SorterOp::sortInQrs(RequestPtr &request,
                            SessionResource *sessionResource,
                            QueryResource *queryResource,
                            MatchDocsVariant *matchDocsVariant,
                            uint32_t extraRankCount,
                            uint32_t &totalMatchDocs,
                            uint32_t &actualMatchDocs,
                            uint32_t srcCount)
{
    if (0 == matchDocsVariant->getMatchDocs().size()) {
        return true;
    }
    auto tracer = queryResource->getTracer();
    auto pool = request->getPool();
    Ha3MatchDocAllocatorPtr allocatorPtr =
        dynamic_pointer_cast<Ha3MatchDocAllocator>(matchDocsVariant->getAllocator());
    if (!allocatorPtr) {
        HA3_LOG(WARN, "dynamic_pointer_cast matchDocAllocator failed.");
        return false;
    }

    SimpleAttributeExpressionCreator exprCreator(pool, allocatorPtr.get());
    MultiErrorResultPtr errorResult;
    SortExpressionCreator sortExprCreator(NULL, NULL, NULL, errorResult, pool);
    ComparatorCreator compCreator(pool, false);

    DataProvider dataProvider;
    auto requiredTopK = extraRankCount;
    SortParam sortParam(pool);
    vector<matchdoc::MatchDoc>  matchDocVec;
    matchDocsVariant->stealMatchDocs(matchDocVec);
    sortParam.matchDocs.insert(sortParam.matchDocs.begin(),
                               matchDocVec.begin(), matchDocVec.end());
    sortParam.requiredTopK = requiredTopK;
    sortParam.totalMatchCount = totalMatchDocs;
    sortParam.actualMatchCount = actualMatchDocs;

    SorterResource sorterResource;
    fillSorterResourceInfo(sorterResource, request.get(), &sortParam.requiredTopK,
                           &exprCreator, &sortExprCreator, allocatorPtr,
                           &dataProvider, &compCreator, pool, srcCount, tracer);
    SorterWrapper *sorterWrapper = NULL;
    if (_location == HA3_NS(sorter)::SL_QRS) {
        QrsSessionResource *qrsSessionResource =
            dynamic_cast<QrsSessionResource *>(sessionResource);
        if (!qrsSessionResource) {
            HA3_LOG(WARN, "ha3 qrs session resource unavailable");
            return false;
        }
        ClusterSorterManagersPtr clusterSorterManagers =
            qrsSessionResource->_clusterSorterManagers;
        sorterWrapper = createSorterWrapper(request.get(),
                clusterSorterManagers, pool);
    } else {
        SearcherSessionResource *searcherSessionResource =
            dynamic_cast<SearcherSessionResource *>(sessionResource);
        if (!searcherSessionResource) {
            HA3_LOG(WARN, "ha3 searcher session resource unavailable");
            return false;
        }
        HA3_NS(service)::SearcherResourcePtr searcherResource =
            searcherSessionResource->searcherResource;
        if (!searcherResource) {
            HA3_LOG(WARN, "ha3 searcher resource unavailable");
            return false;

        }
        sorterWrapper = createSorterWrapper(request.get(),
                searcherResource->getSorterManager().get(), pool);
    }
    if (!sorterWrapper) {
        HA3_LOG(WARN, "Create sorter failed.");
        return false;
    }
    sorter::SorterProvider sorterProvider(sorterResource);
    if (!sorterWrapper->beginSort(&sorterProvider)) {
        string errorMsg = "Sorter beginRequest error[" +
                          sorterProvider.getErrorMessage() + "]";
        HA3_LOG(WARN, "%s.", errorMsg.c_str());
        return false;
    }
    matchDocsVariant->addSortMetas(sorterProvider.getSortExprMeta());
    sorterWrapper->sort(sortParam);
    uint32_t leftCount = min(sortParam.requiredTopK, (uint32_t)sortParam.matchDocs.size());
    matchDocVec.clear();
    for (uint32_t i = 0; i < leftCount; i++) {
        matchDocVec.emplace_back(sortParam.matchDocs[i]);
    }
    for (uint32_t i = leftCount; i < sortParam.matchDocs.size(); ++i) {
        allocatorPtr->deallocate(sortParam.matchDocs[i]);
    }
    matchDocsVariant->stealMatchDocs(matchDocVec);
    totalMatchDocs = sortParam.totalMatchCount;
    actualMatchDocs = sortParam.actualMatchCount;
    sorterWrapper->endSort();
    POOL_DELETE_CLASS(sorterWrapper);
    return true;
}

void Ha3SorterOp::fillSorterResourceInfo(SorterResource& sorterResource,
                            const Request *request,
                            uint32_t *topK,
                            AttributeExpressionCreatorBase *exprCreator,
                            SortExpressionCreator *sortExprCreator,
                            const Ha3MatchDocAllocatorPtr &allocator,
                            DataProvider *dataProvider,
                            ComparatorCreator *compCreator,
                            Pool *pool,
                            uint32_t resultSourceNum,
                            Tracer *tracer)
{
    auto scoreRef = allocator->findReference<score_t>(SCORE_REF);
    sorterResource.scoreExpression = NULL;
    if (scoreRef) {
        sorterResource.scoreExpression =
            AttributeExpressionFactory::createAttributeExpression(scoreRef, pool);
    }

    sorterResource.location = _location;
    sorterResource.pool = pool;
    sorterResource.request = request;
    sorterResource.attrExprCreator = exprCreator;
    sorterResource.sortExprCreator = sortExprCreator;
    sorterResource.dataProvider = dataProvider;
    sorterResource.matchDocAllocator = allocator;
    sorterResource.fieldInfos = NULL;
    sorterResource.requiredTopk = topK;
    sorterResource.resultSourceNum = resultSourceNum;
    sorterResource.comparatorCreator = compCreator;
    sorterResource.requestTracer = tracer;
    sorterResource.kvpairs = &request->getConfigClause()->getKVPairs();
}

SorterWrapper *Ha3SorterOp::createSorterWrapper(const common::Request *request,
        SorterManager *sorterManager,
        autil::mem_pool::Pool *pool)
{
    std::string sorterName = "DEFAULT";
    auto finalSortClause = request->getFinalSortClause();
    if (finalSortClause) {
        sorterName = finalSortClause->getSortName();
    }
    auto sorter = sorterManager->getSorter(sorterName);
    if (!sorter) {
        return NULL;
    }
    return POOL_NEW_CLASS(pool, SorterWrapper, sorter->clone());
}


SorterWrapper *Ha3SorterOp::createSorterWrapper(const common::Request *request,
        const ClusterSorterManagersPtr &clusterSorterManagersPtr,
        autil::mem_pool::Pool *pool)
{
    if (!clusterSorterManagersPtr) {
        return NULL;
    }
    // get first cluster to create sorter.
    string firstClusterName = request->getConfigClause()->getClusterName(0);
    ClusterSorterManagers::const_iterator it =
        clusterSorterManagersPtr->find(firstClusterName);
    if (it == clusterSorterManagersPtr->end()) {
        string errMsg = "no sorter config for cluster[" + firstClusterName + "].";
        HA3_LOG(WARN, "%s", errMsg.c_str());
        return NULL;
    }
    return createSorterWrapper(request, it->second.get(), pool);
}


HA3_LOG_SETUP(turing, Ha3SorterOp);

REGISTER_KERNEL_BUILDER(Name("Ha3SorterOp")
                        .Device(DEVICE_CPU),
                        Ha3SorterOp);

END_HA3_NAMESPACE(turing);
