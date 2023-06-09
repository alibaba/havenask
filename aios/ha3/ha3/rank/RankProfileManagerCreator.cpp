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
#include "ha3/rank/RankProfileManagerCreator.h"

#include <cstddef>
#include <memory>
#include <vector>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "build_service/plugin/ModuleInfo.h"
#include "build_service/plugin/PlugInManager.h"
#include "ha3/config/RankProfileConfig.h"
#include "ha3/config/ResourceReader.h"
#include "ha3/isearch.h"
#include "ha3/rank/RankProfile.h"
#include "ha3/rank/RankProfileManager.h"
#include "fslib/util/FileUtil.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"

namespace suez {
namespace turing {
class CavaPluginManager;
}  // namespace turing
}  // namespace suez

namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace std;
using namespace build_service::plugin;
using namespace suez::turing;
using namespace isearch::config;
using namespace fslib::util;
namespace isearch {
namespace rank {
AUTIL_LOG_SETUP(ha3, RankProfileManagerCreator);

RankProfileManagerCreator::RankProfileManagerCreator(
        const config::ResourceReaderPtr &resourceReaderPtr,
        const CavaPluginManager *cavaPluginManager,
        kmonitor::MetricsReporter *metricsReporter)
    : _resourceReaderPtr(resourceReaderPtr)
    , _cavaPluginManager(cavaPluginManager)
    , _metricsReporter(metricsReporter)
{
}

RankProfileManagerCreator::~RankProfileManagerCreator() {
}

void RankProfileManagerCreator::addRecordProfileInfo(
        RankProfileInfos& rankProfileInfos)
{
    for (int i = 0; i < (int)rankProfileInfos.size(); i++) {
        if (rankProfileInfos[i].rankProfileName == DEFAULT_DEBUG_RANK_PROFILE) {
            rankProfileInfos.erase(rankProfileInfos.begin() + i);
            i--;
        }
    }
    ScorerInfo scorerInfo;
    scorerInfo.scorerName = DEFAULT_DEBUG_SCORER;
    scorerInfo.rankSize = 10;
    RankProfileInfo profileInfo;
    profileInfo.rankProfileName = DEFAULT_DEBUG_RANK_PROFILE;
    profileInfo.scorerInfos.emplace_back(scorerInfo);
    rankProfileInfos.push_back(profileInfo);
}

RankProfileManagerPtr RankProfileManagerCreator::createFromFile(
        const string &filePath)
{
    string configStr = FileUtil::readFile(filePath);
    return createFromString(configStr);
}

RankProfileManagerPtr RankProfileManagerCreator::createFromString(
        const std::string &configStr)
{
    RankProfileConfig rankProfileConfig;
    if (!configStr.empty()) {
        try {
            FromJsonString(rankProfileConfig, configStr);
        } catch (const ExceptionBase &e){
            AUTIL_LOG(ERROR, "Catch exception in RankProfileManagerCreator: [%s]\n"
                    " FromJsonString fail: [%s]", e.what(), configStr.c_str());
            return RankProfileManagerPtr();
        }
    }
    return create(rankProfileConfig);
}

RankProfileManagerPtr RankProfileManagerCreator::create(const RankProfileConfig &rankProfileConfig) {
    RankProfileInfos rankProfileInfos = rankProfileConfig.getRankProfileInfos();
    RankProfileManagerPtr rankProfileManagerPtr(
            new RankProfileManager(_resourceReaderPtr));
    if (!rankProfileManagerPtr->addModules(rankProfileConfig.getModuleInfos())) {
        AUTIL_LOG(ERROR, "Load scorer module failed : %s",
                  ToJsonString(rankProfileConfig.getModuleInfos()).c_str());
        return rankProfileManagerPtr;
    }
    addRecordProfileInfo(rankProfileInfos); //for analyze query match
    for (size_t i = 0; i < rankProfileInfos.size(); i++) {
        rankProfileManagerPtr->addRankProfile(new RankProfile(rankProfileInfos[i]));
    }
    if (!rankProfileManagerPtr->init(_cavaPluginManager, _metricsReporter)) {
        AUTIL_LOG(ERROR, "RankProfileManager init failed!");
        return RankProfileManagerPtr();
    }
    return rankProfileManagerPtr;
}

} // namespace rank
} // namespace isearch
