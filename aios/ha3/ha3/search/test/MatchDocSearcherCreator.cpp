#include <ha3/search/test/MatchDocSearcherCreator.h>
#include <ha3/config/SearcherCacheConfig.h>
#include <suez/turing/expression/function/FuncConfig.h>
#include <ha3/config/ResourceReader.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <suez/turing/expression/plugin/SorterManager.h>
#include <ha3/search/OptimizerChainManagerCreator.h>
#include <ha3/search/test/FakeAttributeExpressionCreator.h>
#include <ha3/test/test.h>

USE_HA3_NAMESPACE(func_expression);
USE_HA3_NAMESPACE(sorter);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(monitor);
using namespace build_service::plugin;
using namespace suez::turing;
USE_HA3_NAMESPACE(rank);

using namespace std;
using namespace autil;

BEGIN_HA3_NAMESPACE(search);
HA3_LOG_SETUP(search, MatchDocSearcherCreator);

MatchDocSearcherCreator::MatchDocSearcherCreator(autil::mem_pool::Pool *pool)
{
    _pool = pool;
    _resourceReaderPtr.reset(new ResourceReader(TEST_DATA_PATH"/searcher_test"));
    _rankProfileMgrPtr = makeFakeRankProfileMgr();
    assert(_rankProfileMgrPtr);

    _tableInfoPtr= makeFakeTableInfo();
    assert(_tableInfoPtr);

    _funcCreatorPtr.reset(new FunctionInterfaceCreator);
    FuncConfig fc;
    _funcCreatorPtr->init(fc, {});

    OptimizerChainManagerCreator optimizerChainManagerCreator(_resourceReaderPtr);
    SearchOptimizerConfig optimizerConfig;
    _optimizerChainManagerPtr = optimizerChainManagerCreator.create(optimizerConfig);

    SorterConfig finalSorterConfig;
    // add buildin sorters
    build_service::plugin::ModuleInfo moduleInfo;
    finalSorterConfig.modules.push_back(moduleInfo);
    suez::turing::SorterInfo sorterInfo;
    sorterInfo.sorterName = "DEFAULT";
    sorterInfo.className = "DefaultSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    sorterInfo.sorterName = "NULL";
    sorterInfo.className = "NullSorter";
    finalSorterConfig.sorterInfos.push_back(sorterInfo);
    _sorterManager = SorterManager::create(_resourceReaderPtr->getConfigPath(),
                                           finalSorterConfig);

    _sessionMetricsCollectorPtr.reset(new SessionMetricsCollector(NULL));

    _partCount = 1;
}

MatchDocSearcherCreator::~MatchDocSearcherCreator() {
}

MatchDocSearcherPtr MatchDocSearcherCreator::createSearcher(
        common::Request *request,
        const IndexPartitionReaderWrapperPtr &indexPartReaderPtr,
        const SearcherCacheConfig &cacheConfig,
        const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &snapshot,
        rank::RankProfileManager* rankProfileManager)
{
    _cachePtr.reset(new SearcherCache());
    bool ret = _cachePtr->init(cacheConfig);
    (void)ret;
    assert(ret);
    return createSearcher(request, indexPartReaderPtr, _cachePtr.get(), snapshot, rankProfileManager);
}

MatchDocSearcherPtr MatchDocSearcherCreator::createSearcher(
        common::Request *request,
        const IndexPartitionReaderWrapperPtr &indexPartReaderPtr,
        SearcherCache *searcherCache,
        const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &snapshot,
        RankProfileManager* rankProfileManager,
        const LayerMetasPtr &layerMetas)
{
    _resource.reset(new SearchCommonResource(_pool, _tableInfoPtr,
                    _sessionMetricsCollectorPtr.get(),
                    NULL, NULL,
                    _funcCreatorPtr.get(), suez::turing::CavaPluginManagerPtr(),
                    request, NULL, _cavaJitModules));
    _partitionResource.reset(
            new SearchPartitionResource(*_resource, request, indexPartReaderPtr, snapshot));

    _partitionResource->attributeExpressionCreator.reset(new FakeAttributeExpressionCreator(
                    _pool, indexPartReaderPtr, _funcCreatorPtr.get(),
                    &_partitionResource->functionProvider,
                    request->getVirtualAttributeClause(),
                    NULL, _resource->matchDocAllocator->getCurrentSubDocRef()));

    _runtimeResource = createSearchRuntimeResource(request,
            rankProfileManager ? rankProfileManager : _rankProfileMgrPtr.get(),
            _resource, _partitionResource->attributeExpressionCreator.get());

    MatchDocSearcherPtr searcherPtr(
            new MatchDocSearcher(*_resource, *_partitionResource, *_runtimeResource,
                    rankProfileManager ? rankProfileManager : _rankProfileMgrPtr.get(),
                    _optimizerChainManagerPtr.get(),
                    _sorterManager.get(), _aggSamplerConfigInfo, _clusterConfigInfo,
                    _partCount, searcherCache, NULL, layerMetas));

    return searcherPtr;
}

SearchRuntimeResourcePtr MatchDocSearcherCreator::createSearchRuntimeResource(
        const common::Request *request,
        RankProfileManager *rankProfileMgr,
        const SearchCommonResourcePtr &commonResource,
        AttributeExpressionCreator *attributeExpressionCreator)
{
    SearchRuntimeResourcePtr runtimeResource;
    const RankProfile *rankProfile = NULL;
    if (!rankProfileMgr->getRankProfile(request, rankProfile, commonResource->errorResult)) {
        HA3_LOG(WARN, "Get RankProfile Failed");
        //return runtimeResource;
    }
    SortExpressionCreatorPtr exprCreator(new SortExpressionCreator(
                    attributeExpressionCreator, rankProfile,
                    commonResource->matchDocAllocator.get(),
                    commonResource->errorResult, commonResource->pool));
    if (!exprCreator->init(request)) {
        HA3_LOG(WARN, "init sort expression creator failed.");
        //return runtimeResource;
    }
    runtimeResource.reset(new SearchRuntimeResource());
    runtimeResource->sortExpressionCreator = exprCreator;
    runtimeResource->comparatorCreator.reset(new ComparatorCreator(
                    commonResource->pool,
                    request->getConfigClause()->isOptimizeComparator()));
    runtimeResource->docCountLimits.init(request, rankProfile,
            _clusterConfigInfo,
            _partCount, commonResource->tracer);
    return runtimeResource;
}


MatchDocSearcherPtr MatchDocSearcherCreator::createSearcher(
        common::Request *request,
        const IndexPartitionReaderWrapperPtr &indexPartReaderPtr,
        const IE_NAMESPACE(partition)::PartitionReaderSnapshotPtr &snapshot,
        RankProfileManager* rankProfileManager,
        const LayerMetasPtr &layerMetas)
{
    return createSearcher(request, indexPartReaderPtr, nullptr, snapshot, rankProfileManager, layerMetas);
}

TableInfoPtr MatchDocSearcherCreator::makeFakeTableInfo() {
    TableInfoConfigurator tableInfoConfigurator;
    TableInfoPtr tableInfoPtr = tableInfoConfigurator.createFromFile(TEST_DATA_PATH"/searcher_test_schema.json");

    assert(tableInfoPtr);

    //modify the 'Analyzer' setting of 'phrase' index. (temp solution)
    IndexInfos *indexInfos = (IndexInfos *)tableInfoPtr->getIndexInfos();
    assert(indexInfos);
    IndexInfo *indexInfo = (IndexInfo *)indexInfos->getIndexInfo("phrase");
    assert(indexInfo);
    indexInfo->setAnalyzerName("default");

    tableInfoPtr->setPrimaryKeyAttributeFlag(false);
    return tableInfoPtr;
}

rank::RankProfileManagerPtr MatchDocSearcherCreator::makeFakeRankProfileMgr() {
    PlugInManagerPtr plugInMgrPtr(new PlugInManager(_resourceReaderPtr));
    ModuleInfo moduleInfo;
    moduleInfo.moduleName = "fakefullcachescorer";
    moduleInfo.modulePath = "libfakefullcachescorer.so";
    ModuleInfos moduleInfos;
    moduleInfos.push_back(moduleInfo);
    bool ret = plugInMgrPtr->addModules(moduleInfos);
    (void)ret;
    assert(ret);

    RankProfileManagerPtr rankProfileMgrPtr(
            new RankProfileManager(plugInMgrPtr));
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = "FakeFullCacheScorer";
    scorerInfo.moduleName = "fakefullcachescorer";
    scorerInfo.rankSize = 3000;
    scorerInfo.parameters["sleep_time"] = "10";
    ScorerInfos scorerInfos;
    scorerInfos.push_back(scorerInfo);

    if (!rankProfileMgrPtr->addRankProfile(
                    new RankProfile("DefaultProfile", scorerInfos)))
    {
        HA3_LOG(ERROR, "add rankprofile for test failed");
        return RankProfileManagerPtr();
    }

    CavaPluginManagerPtr cavaPluginManagerPtr;
    if (!rankProfileMgrPtr->init(_resourceReaderPtr, cavaPluginManagerPtr, NULL)) {
        return RankProfileManagerPtr();
    }

    return rankProfileMgrPtr;
}


END_HA3_NAMESPACE(search);
