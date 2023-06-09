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
#include "ha3/rank/RankProfileManager.h"

#include <stdint.h>
#include <cstddef>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "ha3/common/ConfigClause.h"
#include "ha3/common/ErrorDefine.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/common/RankClause.h"
#include "ha3/common/RankSortClause.h"
#include "ha3/common/RankSortDescription.h"
#include "ha3/common/Request.h"
#include "ha3/common/SortClause.h"
#include "ha3/common/SortDescription.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/index/index.h"
#include "ha3/isearch.h"
#include "ha3/rank/RankProfile.h"
#include "indexlib/misc/common.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/util/TableInfo.h"
#include "suez/turing/expression/plugin/ScorerModuleFactory.h"

namespace suez {
namespace turing {
class CavaPluginManager;
}  // namespace turing
}  // namespace suez

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

using namespace std;
using namespace indexlib::index;
using namespace isearch::rank;
using namespace build_service::plugin;
using namespace suez::turing;
using namespace isearch::config;
namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, RankProfileManager);

RankProfileManager::RankProfileManager(
        const config::ResourceReaderPtr &resourceReaderPtr)
    : PlugInManager(resourceReaderPtr, suez::turing::MODULE_FUNC_SCORER)
{
}

RankProfileManager::~RankProfileManager() {
    for (RankProfiles::iterator it = _rankProfiles.begin();
         it != _rankProfiles.end(); it++)
    {
        delete it->second;
    }
}

bool RankProfileManager::init(const CavaPluginManager *cavaPluginManager,
                              kmonitor::MetricsReporter *metricsReporter)
{
    AUTIL_LOG(TRACE3, "Init rankprofiles, num [%ld]", _rankProfiles.size());
    for (RankProfiles::iterator it = _rankProfiles.begin();
         it != _rankProfiles.end(); it++)
    {
        RankProfile *rankProfile = it->second;
        if (!rankProfile->init(this, _resourceReaderPtr, cavaPluginManager,
                               metricsReporter))
        {
            AUTIL_LOG(ERROR, "Init rankprofile [%s] failed!", it->first.c_str());
            return false;
        }
    }

    return true;
}

void RankProfileManager::mergeFieldBoostTable(const TableInfo &tableInfo) {
    for (RankProfiles::iterator it = _rankProfiles.begin();
         it != _rankProfiles.end(); it++)
    {
        RankProfile *rankProfile = it->second;
        rankProfile->mergeFieldBoostTable(tableInfo);
    }
}

RankProfile* RankProfileManager::getRankProfile(const string &profileName) const {
    RankProfiles::const_iterator it = _rankProfiles.find(profileName);
    if (it == _rankProfiles.end()) {
        return NULL;
    }
    return it->second;
}

bool RankProfileManager::addRankProfile(RankProfile *rankProfile) {
    AUTIL_LOG(TRACE3, "RankProfileManager addRankProfile. RankProfileName:[%s]",
        rankProfile->getProfileName().c_str());

    _rankProfiles.insert(make_pair(rankProfile->getProfileName(), rankProfile));
    return true;
}

bool RankProfileManager::getRankProfile(const common::Request *request,
        const RankProfile*& rankProfile, const common::MultiErrorResultPtr& errorResult) const
{
    if (!needCreateRankProfile(request)) {
        return true;
    }
    std::string profileName = DEFAULT_RANK_PROFILE;
    const common::ConfigClause *configClause = request->getConfigClause();
    if (!configClause->getDebugQueryKey().empty()) {
        profileName = DEFAULT_DEBUG_RANK_PROFILE; //for debug query match
    } else {
        std::string specifiedProfileName = getRankProfileName(request);
        if (!specifiedProfileName.empty()) {
            profileName = specifiedProfileName;
        }
    }
    rankProfile = getRankProfile(profileName);
    if (!rankProfile) {
        std::string errorMsg = std::string("Get rank profile failed, profileName=")
                               + profileName;
        errorResult->addError(ERROR_NO_RANKPROFILE, errorMsg);
        AUTIL_LOG(WARN, "%s", errorMsg.c_str());
        return false;
    }
    return true;
}

bool RankProfileManager::needCreateRankProfile(const common::Request *request) {
    std::string rankProfileName = getRankProfileName(request);
    if (request->getConfigClause()->needRerank() ||
        isRankRequiredInRequest(request) || !rankProfileName.empty()) {
        return true;
    }
    return false;
}

bool RankProfileManager::isRankRequiredInRequest(const common::Request *request)  {
    common::SortClause *sortClause = request->getSortClause();
    common::RankSortClause *rankSortClause = request->getRankSortClause();

    if (!sortClause && !rankSortClause) {
        return true;
    }
    if (sortClause) {
        const std::vector<common::SortDescription *> &sortDescriptions
            = sortClause->getSortDescriptions();
        for (std::vector<common::SortDescription *>::const_iterator it = sortDescriptions.begin();
             it != sortDescriptions.end(); it++)
        {
            if ((*it)->isRankExpression()) {
                return true;
            }
        }
    }
    if (rankSortClause) {
        uint32_t rankSortDescCount = rankSortClause->getRankSortDescCount();
        for (size_t i = 0; i < rankSortDescCount; ++i) {
            const std::vector<common::SortDescription*> &sortDescriptions =
                rankSortClause->getRankSortDesc(i)->getSortDescriptions();
            for (std::vector<common::SortDescription *>::const_iterator it = sortDescriptions.begin();
                 it != sortDescriptions.end(); it++) {
                if ((*it)->isRankExpression()) {
                    return true;
                }
            }
        }
    }
    return false;
}

std::string RankProfileManager::getRankProfileName(const common::Request *request) {
    std::string rankProfileName;
    common::RankClause *rankClause = request->getRankClause();
    if (rankClause) {
        rankProfileName = rankClause->getRankProfileName();
    }
    return rankProfileName;
}

} // namespace rank
} // namespace isearch
