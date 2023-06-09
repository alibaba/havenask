/*
 * Copyright 2014-present Alibaba Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "ha3/service/SearcherResourceCreator.h"

#include <assert.h>
#include <cstddef>
#include <memory>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "build_service/config/ResourceReader.h"
#include "indexlib/misc/common.h"
#include "indexlib/partition/partition_define.h"
#include "suez/turing/common/SessionResource.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/util/AttributeInfos.h"
#include "suez/turing/expression/util/SummaryInfo.h"
#include "suez/turing/search/Biz.h"

#include "ha3/common/ErrorDefine.h"
#include "ha3/common/ErrorResult.h"
#include "ha3/common/ReturnInfo.h"
#include "ha3/config/AggSamplerConfigInfo.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/RankProfileConfig.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/config/SearchOptimizerConfig.h"
#include "ha3/config/SearcherCacheConfig.h"
#include "ha3/config/ServiceDegradationConfig.h"
#include "ha3/config/SummaryProfileConfig.h"
#include "ha3/isearch.h"
#include "ha3/rank/RankProfileManagerCreator.h"
#include "ha3/search/Optimizer.h"
#include "ha3/search/OptimizerChainManagerCreator.h"
#include "ha3/search/SearcherCache.h"
#include "ha3/service/SearcherResource.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/summary/SummaryProfileManagerCreator.h"
#include "autil/Log.h"
#include "ha3/util/memcache/atomic.h"

namespace isearch {
namespace proto {
class Range;
}  // namespace proto
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

using namespace autil;
using namespace suez::turing;

namespace isearch {
namespace service {
AUTIL_LOG_SETUP(ha3, SearcherResourceCreator);

using namespace isearch::config;
using namespace isearch::search;
using namespace isearch::rank;
using namespace isearch::summary;
using namespace indexlib::index;
using namespace isearch::common;
using namespace isearch::util;
using namespace indexlib::index;
using namespace indexlib::config;

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
{
}

SearcherResourceCreator::~SearcherResourceCreator() {
}

ReturnInfo SearcherResourceCreator::createSearcherResource(
        const string &clusterName,
        const string &mainTableIndexRoot,
        SearcherResourcePtr &searcherResourcePtr)
{
    ReturnInfo ret;
    _resourceReaderPtr.reset(new ResourceReader(_configAdapterPtr->getConfigPath()));
    if (!_resourceReaderPtr->initGenerationMeta(mainTableIndexRoot)) {
        AUTIL_LOG_AND_FAILD(ret, "init generation meta.");
        return ret;
    }
    SearcherResourcePtr resourcePtr(new SearcherResource);
    TableInfoPtr tableInfoPtr = _biz->getTableInfo();
    if (!tableInfoPtr) {
        AUTIL_LOG_AND_FAILD(ret, "createTableInfo failed");
        return ret;
    }
    resourcePtr->setTableInfo(tableInfoPtr);
    resourcePtr->setCavaPluginManager(_biz->getCavaPluginManager());

    RankProfileManagerPtr rankProfileMgrPtr;
    if (!createRankConfigMgr(rankProfileMgrPtr, tableInfoPtr, clusterName)) {
        AUTIL_LOG_AND_FAILD(ret, "createRankConfigMgr failed.");
        return ret;
    }
    resourcePtr->setRankProfileManager(rankProfileMgrPtr);

    FunctionInterfaceCreatorPtr funcCreatorPtr = _biz->getFunctionInterfaceCreator();
    if (!funcCreatorPtr) {
        AUTIL_LOG_AND_FAILD(ret, "create function manager failed!!!");
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
        AUTIL_LOG_AND_FAILD(ret, "createSummaryConfigMgr failed!!!");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setSummaryProfileManager(summaryProfileMgrPtr);

    OptimizerChainManagerPtr optimizerChainManagerPtr = createOptimizerChainManager(clusterName);
    if (!optimizerChainManagerPtr) {
        AUTIL_LOG_AND_FAILD(ret, "create optimizer chain manager failed!!!");
        return ERROR_CONFIG_PARSE;
    }

    resourcePtr->setOptimizerChainManager(optimizerChainManagerPtr);
    resourcePtr->setClusterName(clusterName);
    SearcherCachePtr searcherCachePtr;
    if (!createSearcherCache(clusterName, searcherCachePtr)) {
        AUTIL_LOG_AND_FAILD(ret, "createSearcherCache failed!!!");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setSearcherCache(searcherCachePtr);
    SorterManagerPtr sorterManagerPtr = _biz->getSorterManager();
    if (!sorterManagerPtr) {
        AUTIL_LOG_AND_FAILD(ret, "create SorterManager failed!!!");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setSorterManager(sorterManagerPtr);

    AggSamplerConfigInfo aggSamplerConfigInfo;
    if (!_configAdapterPtr->getAggSamplerConfigInfo(clusterName, aggSamplerConfigInfo))
    {
        AUTIL_LOG_AND_FAILD(ret, "parse section [aggregate_sampler_config] failed");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setAggSamplerConfigInfo(aggSamplerConfigInfo);

    searcherResourcePtr = resourcePtr;

    ClusterConfigInfo clusterConfigInfo;
    if (!_configAdapterPtr->getClusterConfigInfo(clusterName, clusterConfigInfo)) {
        AUTIL_LOG_AND_FAILD(ret, "get " + clusterName + " [cluster_config] fail");
        return ERROR_CONFIG_PARSE;
    }
    resourcePtr->setClusterConfig(clusterConfigInfo);

    ServiceDegradationConfig serviceDegradationConfig;
    if (!_configAdapterPtr->getServiceDegradationConfig(
                    clusterName, serviceDegradationConfig))
    {
        AUTIL_LOG_AND_FAILD(ret, "get " + clusterName + " [service_degradation_config] fail");
        return ERROR_CONFIG_PARSE;
    }
    ServiceDegradePtr serviceDegradePtr(new ServiceDegrade(serviceDegradationConfig));
    resourcePtr->setServiceDegrade(serviceDegradePtr);

    AUTIL_LOG(DEBUG, "CreateSearcherResource success!");
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
        AUTIL_LOG(ERROR, "Get RankProfileConfig Fail. cluster_name[%s]",
                clusterName.c_str());
        return false;
    }

    RankProfileManagerCreator rankCreator(_resourceReaderPtr,
            _biz->getCavaPluginManager().get(), _metricsReporter);
    rankProfileMgrPtr = rankCreator.create(rankProfileConfig);
    if (!rankProfileMgrPtr) {
        AUTIL_LOG(ERROR, "rankProfileMgrPtr is NULL! clusterName[%s]", clusterName.c_str());
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
        AUTIL_LOG(WARN, "Get SummaryProfileConfig failed. cluster_name[%s]",
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
            hitSummarySchemaPtr, _biz->getCavaPluginManager().get(),
            _metricsReporter);
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
                AUTIL_LOG(ERROR, "attribute[%s] is already in summary schema",
                        attributeName.c_str());
                return false;
            }
        }
        if (!attrInfos) {
            AUTIL_LOG(ERROR, "attribute schema does not exist");
            return false;
        }
        auto attr = attrInfos->getAttributeInfo(attributeName);
        if (!attr) {
            AUTIL_LOG(ERROR, "attribute[%s] does not exist",
                    attributeName.c_str());
            return false;
        }
        if (attr->getSubDocAttributeFlag()) {
            AUTIL_LOG(ERROR, "sub doc attribute is not supported in required_attribute_fields: %s",
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
        AUTIL_LOG(ERROR, "Get searcher cache config in cluster[%s] failed.",
                clusterName.c_str());
        return false;
    }

    if (searcherCacheConfig.maxSize == 0) {
        AUTIL_LOG(INFO, "cache size not set, searcher cache disabled!");
        return true;
    }

    searcherCachePtr.reset(new SearcherCache());
    if (!searcherCachePtr->init(searcherCacheConfig)) {
        AUTIL_LOG(ERROR, "Init searcher cache failed.");
        searcherCachePtr.reset();
        return false;
    }

    return true;
}

OptimizerChainManagerPtr SearcherResourceCreator::createOptimizerChainManager(const string &clusterName) {
    OptimizerChainManagerCreator optimizerChainManagerCreator(_resourceReaderPtr);
    SearchOptimizerConfig optimizerConfig;
    if(!_configAdapterPtr->getSearchOptimizerConfig(clusterName, optimizerConfig)) {
        AUTIL_LOG(ERROR, "Failed to get optimizer manager config in cluster[%s].", clusterName.c_str());
        return OptimizerChainManagerPtr();
    }
    return optimizerChainManagerCreator.create(optimizerConfig);
}

} // namespace service
} // namespace isearch
