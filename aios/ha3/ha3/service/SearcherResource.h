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
#pragma once

#include "indexlib/partition/index_partition.h"
#include "indexlib/partition/partition_reader_snapshot.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/function/FunctionInterfaceCreator.h"
#include "suez/turing/expression/plugin/SorterManager.h"
#include "suez/turing/expression/util/TableInfo.h"

#include "ha3/config/AggSamplerConfigInfo.h"
#include "ha3/config/HitSummarySchema.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/isearch.h"
#include "ha3/proto/BasicDefs.pb.h"
#include "ha3/rank/RankProfileManager.h"
#include "ha3/search/OptimizerChainManager.h"
#include "ha3/service/ServiceDegrade.h"
#include "ha3/summary/SummaryProfileManager.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace search {
class SearcherCache;
typedef std::shared_ptr<SearcherCache> SearcherCachePtr;
} // namespace search
} // namespace isearch

namespace isearch {
namespace service {

class SearcherResource
{
public:
    SearcherResource() {
    }
    ~SearcherResource() {}
public:
    void setClusterName(const std::string &name) {
        _clusterName = name;
    }
    std::string getClusterName() const {
        return _clusterName;
    }
    void setTableInfo(const suez::turing::TableInfoPtr &tableInfoPtr) {
        _tableInfoPtr = tableInfoPtr;
    }
    suez::turing::TableInfoPtr getTableInfo() const {
        return _tableInfoPtr;
    }

    void setRankProfileManager(
            const rank::RankProfileManagerPtr &rankProfileMgrPtr)
    {
        _rankProfileMgrPtr = rankProfileMgrPtr;
    }
    rank::RankProfileManagerPtr getRankProfileManager() const {
        return _rankProfileMgrPtr;
    }

    void setFunctionCreator(const suez::turing::FunctionInterfaceCreatorPtr &funcCreatorPtr) {
        _funcCreatorPtr = funcCreatorPtr;
    }

    suez::turing::FunctionInterfaceCreatorPtr getFuncCreator() const{
        return _funcCreatorPtr;
    }

    void setAggSamplerConfigInfo(const config::AggSamplerConfigInfo
                                 &aggSamplerConfigInfo)
    {
        _aggSamplerConfigInfo = aggSamplerConfigInfo;
    }

    const config::AggSamplerConfigInfo &getAggSamplerConfigInfo() const {
        return _aggSamplerConfigInfo;
    }

    void setSummaryProfileManager(const summary::SummaryProfileManagerPtr
                                  &summaryProfileManagerPtr)
    {
        _summaryProfileManagerPtr = summaryProfileManagerPtr;
    }

    summary::SummaryProfileManagerPtr getSummaryProfileManager() {
        return _summaryProfileManagerPtr;
    }

    void setHitSummarySchema(const config::HitSummarySchemaPtr &hitSummarySchemaPtr) {
        _hitSummarySchemaPtr = hitSummarySchemaPtr;
    }

    const config::HitSummarySchemaPtr &getHitSummarySchema() const {
        return _hitSummarySchemaPtr;
    }

    void setHitSummarySchemaPool(const config::HitSummarySchemaPoolPtr &hitSummarySchemaPool) {
        _hitSummarySchemaPool = hitSummarySchemaPool;
    }

    const config::HitSummarySchemaPoolPtr &getHitSummarySchemaPool() const {
        return _hitSummarySchemaPool;
    }

    void setOptimizerChainManager(const search::OptimizerChainManagerPtr &optimizerChainManagerPtr) {
        _optimizerChainManagerPtr = optimizerChainManagerPtr;
    }

    const search::OptimizerChainManagerPtr &getOptimizerChainManager() const {
        return _optimizerChainManagerPtr;
    }

    void setSearcherCache(search::SearcherCachePtr &searcherCachePtr) {
        _searcherCachePtr = searcherCachePtr;
    }

    search::SearcherCachePtr &getSearcherCache() {
        return _searcherCachePtr;
    }

    void setSorterManager(const suez::turing::SorterManagerPtr &sorterManagerPtr) {
        _sorterManagerPtr = sorterManagerPtr;
    }

    const suez::turing::SorterManagerPtr &getSorterManager() const {
        return _sorterManagerPtr;
    }

    void setClusterConfig(const config::ClusterConfigInfo& clusterConfig) {
        _clusterConfig = clusterConfig;
    }

    const config::ClusterConfigInfo &getClusterConfig() const {
        return _clusterConfig;
    }

    void setServiceDegrade(const ServiceDegradePtr &serviceDegradePtr) {
        _serviceDegradePtr = serviceDegradePtr;
    }

    const ServiceDegradePtr &getServiceDegrade() const {
        return _serviceDegradePtr;
    }

    void setCavaPluginManager(const suez::turing::CavaPluginManagerPtr &cavaPluginManagerPtr) {
        _cavaPluginManagerPtr = cavaPluginManagerPtr;
    }

    const suez::turing::CavaPluginManagerPtr& getCavaPluginManager()
    {
        return _cavaPluginManagerPtr;
    }

private:
    suez::turing::TableInfoPtr _tableInfoPtr;
    rank::RankProfileManagerPtr _rankProfileMgrPtr;
    suez::turing::FunctionInterfaceCreatorPtr _funcCreatorPtr;
    summary::SummaryProfileManagerPtr _summaryProfileManagerPtr;
    config::HitSummarySchemaPtr _hitSummarySchemaPtr;
    config::HitSummarySchemaPoolPtr _hitSummarySchemaPool;
    search::OptimizerChainManagerPtr _optimizerChainManagerPtr;
    std::string _clusterName;
    config::AggSamplerConfigInfo _aggSamplerConfigInfo;
    search::SearcherCachePtr _searcherCachePtr;
    config::ClusterConfigInfo _clusterConfig;
    suez::turing::SorterManagerPtr _sorterManagerPtr;
    ServiceDegradePtr _serviceDegradePtr;
    suez::turing::CavaPluginManagerPtr _cavaPluginManagerPtr;
private:
    AUTIL_LOG_DECLARE();
private:
    friend class SearcherResourceCreatorTest;
};

typedef std::shared_ptr<SearcherResource> SearcherResourcePtr;

} // namespace service
} // namespace isearch
