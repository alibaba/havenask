#include <ha3/turing/ops/Ha3ResourceUtil.h>

BEGIN_HA3_NAMESPACE(turing);
HA3_LOG_SETUP(turing, Ha3ResourceUtil);

USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(monitor);
USE_HA3_NAMESPACE(service);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(search);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(partition);
using namespace tensorflow;
using namespace suez::turing;
using namespace autil::mem_pool;

SearchCommonResourcePtr Ha3ResourceUtil::createSearchCommonResource(
        const Request *request,
        Pool *pool,
        SessionMetricsCollector *metricsCollector,
        TimeoutTerminator *timeoutTerminator,
        Tracer *tracer,
        SearcherResourcePtr &resourcePtr,
        SuezCavaAllocator *cavaAllocator,
        const std::map<size_t, ::cava::CavaJitModulePtr> &cavaJitModules)
{
    TableInfoPtr tableInfoPtr = resourcePtr->getTableInfo();
    FunctionInterfaceCreatorPtr funcCreatorPtr = resourcePtr->getFuncCreator();
    OptimizerChainManagerPtr optimizerChainManagerPtr = resourcePtr->getOptimizerChainManager();
    SorterManagerPtr sorterManager = resourcePtr->getSorterManager();
    SearcherCachePtr searcherCachePtr = resourcePtr->getSearcherCache();
    SearchCommonResourcePtr commonResource(
            new SearchCommonResource(pool, tableInfoPtr, metricsCollector,
                    timeoutTerminator, tracer, funcCreatorPtr.get(),
                    resourcePtr->getCavaPluginManager(),
                    request, cavaAllocator, cavaJitModules));
    return commonResource;
}

SearchPartitionResourcePtr Ha3ResourceUtil::createSearchPartitionResource(
        const Request *request,
        const IndexPartitionReaderWrapperPtr &indexPartReaderWrapperPtr,
        const PartitionReaderSnapshotPtr &partitionReaderSnapshot,
        SearchCommonResourcePtr &commonResource)
{
    SearchPartitionResourcePtr partitionResource(
            new SearchPartitionResource(*commonResource, request,
                    indexPartReaderWrapperPtr, partitionReaderSnapshot));
    return partitionResource;
}

SearchRuntimeResourcePtr Ha3ResourceUtil::createSearchRuntimeResource(
        const Request *request,
        const SearcherResourcePtr &searcherResource,
        const SearchCommonResourcePtr &commonResource,
        AttributeExpressionCreator *attributeExpressionCreator)
{
    SearchRuntimeResourcePtr runtimeResource;
    RankProfileManagerPtr rankProfileMgrPtr = searcherResource->getRankProfileManager();
    const RankProfile *rankProfile = NULL;
    if (!rankProfileMgrPtr->getRankProfile(request, rankProfile, commonResource->errorResult)) {
        HA3_LOG(WARN, "Get RankProfile Failed");
        return runtimeResource;
    }
    SortExpressionCreatorPtr exprCreator(new SortExpressionCreator(
                    attributeExpressionCreator, rankProfile,
                    commonResource->matchDocAllocator.get(),
                    commonResource->errorResult, commonResource->pool));

    runtimeResource.reset(new SearchRuntimeResource());
    runtimeResource->sortExpressionCreator = exprCreator;
    runtimeResource->comparatorCreator.reset(new ComparatorCreator(
                    commonResource->pool,
                    request->getConfigClause()->isOptimizeComparator()));

    runtimeResource->docCountLimits.init(request, rankProfile,
            searcherResource->getClusterConfig(),
            searcherResource->getPartCount(), commonResource->tracer);
    runtimeResource->rankProfile = rankProfile;
    return runtimeResource;
}


END_HA3_NAMESPACE(turing);
