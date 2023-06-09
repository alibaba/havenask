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

#include "suez/turing/expression/cava/common/CavaJitWrapper.h"
#include "suez/turing/expression/plugin/SorterModuleFactory.h"

#include "ha3/common/ReturnInfo.h"
#include "ha3/config/ConfigAdapter.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "ha3/service/SearcherResource.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace suez {
namespace turing {
class Biz;
}
}
namespace isearch {
namespace service {

class SearcherResourceCreator
{
public:
    SearcherResourceCreator(const config::ConfigAdapterPtr &configAdapterPtr,
                            kmonitor::MetricsReporter *metricsReporter,
                            suez::turing::Biz *biz);
    ~SearcherResourceCreator();

public:
    common::ReturnInfo createSearcherResource(
            const std::string &clusterName,
            const std::string &mainTableIndexRoot,
            SearcherResourcePtr &searcherResourcePtr);

private:
    bool createRankConfigMgr(rank::RankProfileManagerPtr &rankProfileMgrPtr,
                             const suez::turing::TableInfoPtr &tableInfo,
                             const std::string &clusterName);

    config::HitSummarySchemaPtr createHitSummarySchema(const suez::turing::TableInfoPtr &tableInfoPtr);

    search::OptimizerChainManagerPtr createOptimizerChainManager(const std::string &clusterName);

    bool createSummaryConfigMgr(summary::SummaryProfileManagerPtr &summaryProfileMgrPtr,
                                config::HitSummarySchemaPtr &hitSummarySchemaPtr,
                                const std::string &clusterName,
                                const suez::turing::TableInfoPtr &tableInfoPtr);

    bool createSearcherCache(const std::string &clusterName,
                             search::SearcherCachePtr &searcherCachePtr);

    static bool validateAttributeFieldsForSummaryProfile(
            const config::SummaryProfileConfig &summaryProfileConfig,
            const suez::turing::TableInfoPtr &tableInfoPtr);

private:
    config::ConfigAdapterPtr _configAdapterPtr;
    kmonitor::MetricsReporter *_metricsReporter;
    suez::turing::Biz *_biz;
    config::ResourceReaderPtr _resourceReaderPtr;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<SearcherResourceCreator> SearcherResourceCreatorPtr;

} // namespace service
} // namespace isearch
