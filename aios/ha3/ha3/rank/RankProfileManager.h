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

#include <map>
#include <memory>
#include <string>

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/plugin/PlugInManager.h"
#include "ha3/common/MultiErrorResult.h"
#include "ha3/config/ResourceReader.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace suez {
namespace turing {
class CavaPluginManager;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace rank {
class RankProfile;
}  // namespace rank
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor

namespace isearch {
namespace common {
class Request;

} // namespace common
} // namespace isearch

namespace isearch {
namespace rank {

class RankProfileManager : public build_service::plugin::PlugInManager
{
public:
    RankProfileManager(const config::ResourceReaderPtr &resourceReaderPtr);
    ~RankProfileManager();
public:
    typedef std::map<std::string, RankProfile*> RankProfiles;
public:
    bool getRankProfile(const common::Request *request,
                        const RankProfile*& rankProfile,
                        const common::MultiErrorResultPtr& errorResult) const;
public:
    bool init(const suez::turing::CavaPluginManager *cavaPluginManager,
              kmonitor::MetricsReporter *_metricsReporter);
    void mergeFieldBoostTable(const suez::turing::TableInfo &tableInfo);
    bool addRankProfile(RankProfile *rankProfile);
private:
    RankProfile* getRankProfile(const std::string &profileName) const;
private:
    static bool isRankRequiredInRequest(const common::Request *request);
    static std::string getRankProfileName(const common::Request *request);
    static bool needCreateRankProfile(const common::Request *request);
private:
    RankProfiles _rankProfiles;
private:
    friend class RankProfileManagerTest;
    friend class RankProfileManagerCreatorTest;
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<RankProfileManager> RankProfileManagerPtr;

} // namespace rank
} // namespace isearch
