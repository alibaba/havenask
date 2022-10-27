#include <ha3/service/SearcherResourceCreator.h>
#include <suez/turing/expression/util/TableInfoConfigurator.h>
#include <ha3/rank/RankProfileManagerCreator.h>
#include <ha3/summary/SummaryProfileManagerCreator.h>
#include <autil/StringUtil.h>
#include <indexlib/partition/index_partition.h>
#include <indexlib/config/index_partition_options.h>
#include <ha3/search/SearcherCache.h>
#include <ha3/search/OptimizerChainManagerCreator.h>
#include <ha3/config/ClusterTableInfoManager.h>
#include <suez/turing/common/FileUtil.h>

using namespace autil;
using namespace suez::turing;

BEGIN_HA3_NAMESPACE(service);
HA3_LOG_SETUP(service, SearcherResourceCreator);

USE_HA3_NAMESPACE(config);
USE_HA3_NAMESPACE(search);
USE_HA3_NAMESPACE(rank);
USE_HA3_NAMESPACE(summary);
USE_HA3_NAMESPACE(index);
USE_HA3_NAMESPACE(common);
USE_HA3_NAMESPACE(util);
USE_HA3_NAMESPACE(func_expression);
IE_NAMESPACE_USE(index);
IE_NAMESPACE_USE(config);

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;

SearcherResourceCreator::SearcherResourceCreator(
        const ConfigAdapterPtr &configAdapterPtr,
        kmonitor::MetricsReporter *metricsReporter,
        suez::turing::Biz *biz)
    : _configAdapterPtr(configAdapterPtr)
    , _metricsReporter(metricsReporter)
    , _biz(biz)
    , _pluginDir(_configAdapterPtr->getPluginPath())
{
}

SearcherResourceCreator::~SearcherResourceCreator() {
}

ReturnInfo SearcherResourceCreator::createSearcherResource(
        const string &clusterName,
        const proto::Range &hashIdRange,
        FullIndexVersion fullIndexVersion,
        uint32_t partCount,
        const IndexPartitionWrapperPtr &indexPartitionWrapperPtr,
        SearcherResourcePtr &searcherResourcePtr)
{
    ReturnInfo ret;
    string indexRootDir = indexPartitionWrapperPtr->getIndexRoot();
    _resourceReaderPtr.reset(new ResourceReader(
                    _configAdapterPtr->getConfigPath()));
    if (!_resourceReaderPtr->initGenerationMeta(indexRootDir)) {
        HA3_LOG_AND_FAILD(ret, "init generation meta.");
        return ret;
    }
    SearcherResourcePtr resourcePtr(new SearcherResource);
    resourcePtr->setIndexPartitionWrapper(indexPartitionWrapperPtr);

    TableInfoPtr tableInfoPtr = _biz->getTableInfo();
    if (!tableInfoPtr) {
        HA3_LOG_AND_FAILD(ret, "createTableInfo failed");
        return ret;
    }
    resourcePtr->setTableInfo(tableInfoPtr);
    resourcePtr->setCavaPluginManager(_biz->getCavaPluginManager());

    RankProfileManagerPtr rankProfileMgrPtr;
    if (!createRankConfigMgr(rankProfileMgrPtr, tableInfoPtr, clusterName)) {
        HA3_LOG_AND_FAILD(ret, "createRankConfigMgr failed.");
        return ret;
    }
    resourcePtr->setRankProfileManager(rankProfileMgrPtr);

    FunctionInterfaceCreatorPtr funcCreatorPtr = _biz->getFunctionInterfaceCreator();
    if (!funcCreatorPtr) {
        HA3_LOG_AND_FAILD(ret, "create function manager failed!!!");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setFunctionCreator(funcCreatorPtr);

    HitSummarySchemaPtr hitSummarySchemaPtr = createHitSummarySchema(tableInfoPtr);
    assert(hitSummarySchemaPtr);
    resourcePtr->setHitSummarySchema(hitSummarySchemaPtr);
    HitSummarySchemaPoolPtr hitSummarySchemaPool(new HitSummarySchemaPool(hitSummarySchemaPtr));
    resourcePtr->setHitSummarySchemaPool(hitSummarySchemaPool);

    SummaryProfileManagerPtr summaryProfileMgrPtr;
    if (!createSummaryConfigMgr(summaryProfileMgrPtr, hitSummarySchemaPtr,
                                clusterName, tableInfoPtr))
    {
        HA3_LOG_AND_FAILD(ret, "createSummaryConfigMgr failed!!!");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setSummaryProfileManager(summaryProfileMgrPtr);

    OptimizerChainManagerPtr optimizerChainManagerPtr = createOptimizerChainManager(clusterName);
    if (!optimizerChainManagerPtr) {
        HA3_LOG_AND_FAILD(ret, "create optimizer chain manager failed!!!");
        return ERROR_CONFIG_PARSE;
    }

    resourcePtr->setOptimizerChainManager(optimizerChainManagerPtr);
    resourcePtr->setHashIdRange(hashIdRange);
    resourcePtr->setFullIndexVersion(fullIndexVersion);
    resourcePtr->setPartCount(partCount);
    resourcePtr->setClusterName(clusterName);
    SearcherCachePtr searcherCachePtr;
    if (!createSearcherCache(clusterName, searcherCachePtr)) {
        HA3_LOG_AND_FAILD(ret, "createSearcherCache failed!!!");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setSearcherCache(searcherCachePtr);
    SorterManagerPtr sorterManagerPtr = _biz->getSorterManager();
    if (!sorterManagerPtr) {
        HA3_LOG_AND_FAILD(ret, "create SorterManager failed!!!");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setSorterManager(sorterManagerPtr);

    AggSamplerConfigInfo aggSamplerConfigInfo;
    if (!_configAdapterPtr->getAggSamplerConfigInfo(clusterName, aggSamplerConfigInfo))
    {
        HA3_LOG_AND_FAILD(ret, "parse section [aggregate_sampler_config] failed");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setAggSamplerConfigInfo(aggSamplerConfigInfo);

    searcherResourcePtr = resourcePtr;

    ClusterConfigInfo clusterConfigInfo;
    if (!_configAdapterPtr->getClusterConfigInfo(clusterName, clusterConfigInfo)) {
        HA3_LOG_AND_FAILD(ret, "get " + clusterName + " [cluster_config] fail");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setClusterConfig(clusterConfigInfo);

    ServiceDegradationConfig serviceDegradationConfig;
    if (!_configAdapterPtr->getServiceDegradationConfig(
                    clusterName, serviceDegradationConfig))
    {
        HA3_LOG_AND_FAILD(ret, "get " + clusterName + " [service_degradation_config] fail");
        return ERROR_CONFIG_PARSE;
    }
    ServiceDegradePtr serviceDegradePtr(new ServiceDegrade(serviceDegradationConfig));
    resourcePtr->setServiceDegrade(serviceDegradePtr);

    HA3_LOG(DEBUG, "CreateSearcherResource success!");
    return ERROR_NONE;
}

bool SearcherResourceCreator::createRankConfigMgr(
        RankProfileManagerPtr &rankProfileMgrPtr,
        const TableInfoPtr &tableInfoPtr,
        const string &clusterName)
{
    RankProfileConfig rankProfileConfig;
    if (!_configAdapterPtr->getRankProfileConfig(clusterName,
                    rankProfileConfig))
    {
        HA3_LOG(ERROR, "Get RankProfileConfig Fail. cluster_name[%s]",
                clusterName.c_str());
        return false;
    }

    RankProfileManagerCreator rankCreator(_resourceReaderPtr,
            _biz->getCavaPluginManager(), _metricsReporter);
    rankProfileMgrPtr = rankCreator.create(rankProfileConfig);
    if (!rankProfileMgrPtr) {
        HA3_LOG(ERROR, "rankProfileMgrPtr is NULL! clusterName[%s]", clusterName.c_str());
        return false;
    }
    rankProfileMgrPtr->mergeFieldBoostTable(*tableInfoPtr);

    return true;
}

HitSummarySchemaPtr SearcherResourceCreator::createHitSummarySchema(
        const TableInfoPtr &tableInfoPtr)
{
    return HitSummarySchemaPtr(new HitSummarySchema(tableInfoPtr.get()));
}

bool SearcherResourceCreator::createSummaryConfigMgr(
        SummaryProfileManagerPtr &summaryProfileMgrPtr,
        HitSummarySchemaPtr &hitSummarySchemaPtr,
        const string& clusterName,
        const TableInfoPtr &tableInfoPtr)
{
    SummaryProfileConfig summaryProfileConfig;
    if (!_configAdapterPtr->getSummaryProfileConfig(clusterName,
                    summaryProfileConfig))
    {
        HA3_LOG(WARN, "Get SummaryProfileConfig failed. cluster_name[%s]",
                clusterName.c_str());
        return false;
    }

    // check attribute fields in tableInfo
    if (!validateAttributeFieldsForSummaryProfile(summaryProfileConfig,
                    tableInfoPtr))
    {
        return false;
    }

    SummaryProfileManagerCreator summaryCreator(_resourceReaderPtr,
            hitSummarySchemaPtr, _biz->getCavaPluginManager(), _metricsReporter);
    summaryProfileMgrPtr = summaryCreator.create(summaryProfileConfig);
    return (summaryProfileMgrPtr != NULL);
}

bool SearcherResourceCreator::validateAttributeFieldsForSummaryProfile(
        const SummaryProfileConfig &summaryProfileConfig,
        const TableInfoPtr &tableInfoPtr)
{
    const AttributeInfos *attrInfos = tableInfoPtr->getAttributeInfos();
    const SummaryInfo *summaryInfo = tableInfoPtr->getSummaryInfo();
    const vector<string> &requiredAttrFields =
        summaryProfileConfig.getRequiredAttributeFields();
    for (size_t i = 0; i < requiredAttrFields.size(); i++) {
        const string &attributeName = requiredAttrFields[i];
        if (summaryInfo != NULL) {
            if (summaryInfo->exist(attributeName)) {
                HA3_LOG(ERROR, "attribute[%s] is already in summary schema",
                        attributeName.c_str());
                return false;
            }
        }
        if (!attrInfos) {
            HA3_LOG(ERROR, "attribute schema does not exist");
            return false;
        }
        auto attr = attrInfos->getAttributeInfo(attributeName);
        if (!attr) {
            HA3_LOG(ERROR, "attribute[%s] does not exist",
                    attributeName.c_str());
            return false;
        }
        if (attr->getSubDocAttributeFlag()) {
            HA3_LOG(ERROR, "sub doc attribute is not supported in required_attribute_fields: %s",
                    attributeName.c_str());
            return false;
        }
    }
    return true;
}

bool SearcherResourceCreator::createSearcherCache(const string &clusterName,
        SearcherCachePtr &searcherCachePtr)
{
    SearcherCacheConfig searcherCacheConfig;
    if (!_configAdapterPtr->getSearcherCacheConfig(clusterName,
                    searcherCacheConfig))
    {
        HA3_LOG(ERROR, "Get searcher cache config in cluster[%s] failed.",
                clusterName.c_str());
        return false;
    }

    if (searcherCacheConfig.maxSize == 0) {
        HA3_LOG(INFO, "cache size not set, searcher cache disabled!");
        return true;
    }

    searcherCachePtr.reset(new SearcherCache());
    if (!searcherCachePtr->init(searcherCacheConfig)) {
        HA3_LOG(ERROR, "Init searcher cache failed.");
        searcherCachePtr.reset();
        return false;
    }

    return true;
}

OptimizerChainManagerPtr SearcherResourceCreator::createOptimizerChainManager(const string &clusterName) {
    OptimizerChainManagerCreator optimizerChainManagerCreator(_resourceReaderPtr);
    SearchOptimizerConfig optimizerConfig;
    if(!_configAdapterPtr->getSearchOptimizerConfig(clusterName, optimizerConfig)) {
        HA3_LOG(ERROR, "Failed to get optimizer manager config in cluster[%s].", clusterName.c_str());
        return OptimizerChainManagerPtr();
    }
    return optimizerChainManagerCreator.create(optimizerConfig);
}

END_HA3_NAMESPACE(service);
