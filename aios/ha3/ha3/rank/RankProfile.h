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

#include <stdint.h>
#include <map>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "build_service/plugin/PlugInManager.h"
#include "ha3/config/RankProfileInfo.h"
#include "ha3/config/ResourceReader.h"
#include "suez/turing/expression/cava/common/CavaPluginManager.h"
#include "suez/turing/expression/util/FieldBoostTable.h"
#include "suez/turing/expression/util/TableInfo.h"

namespace autil {
namespace mem_pool {
class Pool;
}  // namespace mem_pool
}  // namespace autil
namespace isearch {
namespace common {
class Request;
}  // namespace common
}  // namespace isearch
namespace kmonitor {
class MetricsReporter;
}  // namespace kmonitor
namespace suez {
namespace turing {
class CavaPluginManager;
class RankAttributeExpression;
class Scorer;
}  // namespace turing
}  // namespace suez

namespace isearch {
namespace rank {

#define FIELD_BOOST_TOKENIZER "."

class RankProfile
{
public:
    RankProfile(const std::string &profileName);
    RankProfile(const std::string &profileName,
                const config::ScorerInfos& scorerInfos);
    RankProfile(const config::RankProfileInfo &info);
    ~RankProfile();

// for RankProfileManager:
public:
    bool init(build_service::plugin::PlugInManager *plugInManagerPtr,
              const config::ResourceReaderPtr &resourceReaderPtr,
              const suez::turing::CavaPluginManager *cavaPluginManager,
              kmonitor::MetricsReporter *metricsReporter);

    /* merge the 'FieldBoostTable' in 'RankProfile' config and 'TableInfo' config. */
    void mergeFieldBoostTable(const suez::turing::TableInfo &tableInfo);
    void addScorerInfo(const config::ScorerInfo &scorerInfo);

// for MatchDocSearcher:
public:
    suez::turing::RankAttributeExpression* createRankExpression(
            autil::mem_pool::Pool *pool) const;
    uint32_t getRankSize(uint32_t phase) const;
    uint32_t getTotalRankSize(uint32_t phase) const;
    int getPhaseCount() const;
    bool getScorerInfo(uint32_t idx, config::ScorerInfo &scorerInfo) const;
    bool getScorers(std::vector<suez::turing::Scorer *> &scores, uint32_t scorePos) const;
    const std::string& getProfileName() const;

    const suez::turing::FieldBoostTable *getFieldBoostTable() const{
        return &_fieldBoostTable;
    }

    void getCavaScorerCodes(std::vector<std::string> &moduleNames,
                            std::vector<std::string> &fileNames,
                            std::vector<std::string> &codes,
                            common::Request *request) const;

private:
    suez::turing::Scorer* createScorer(const config::ScorerInfo &scorerInfo,
            build_service::plugin::PlugInManager *plugInManagerPtr,
            const config::ResourceReaderPtr &resourceReaderPtr,
            const suez::turing::CavaPluginManager *cavaPluginManager,
            kmonitor::MetricsReporter *metricsReporter);
private:
    std::string _profileName;
    config::ScorerInfos _scorerInfos;
    std::map<std::string, uint32_t> _fieldBoostInfo;
    std::vector<suez::turing::Scorer *> _scorers;
    suez::turing::FieldBoostTable _fieldBoostTable;
private:
    friend class RankProfileTest;
private:
    AUTIL_LOG_DECLARE();
};

} // namespace rank
} // namespace isearch
