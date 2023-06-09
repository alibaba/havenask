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
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "autil/Log.h" // IWYU pragma: keep
#include "autil/legacy/json.h"
#include "ha3/config/AnomalyProcessConfig.h"
#include "ha3/config/FinalSorterConfig.h"
#include "indexlib/misc/common.h"

namespace suez {
namespace turing {
class CavaConfig;
class FuncConfig;
}  // namespace turing
}  // namespace suez

namespace indexlib {
namespace config {
}
}

namespace isearch {
namespace config {

class AggSamplerConfigInfo;
class ClusterConfigInfo;
class RankProfileConfig;
class SearchOptimizerConfig;
class SearcherCacheConfig;
class ServiceDegradationConfig;
class SummaryProfileConfig;
class TuringOptionsInfo;

// parser for XXX_cluster.json
class ClusterConfigParser
{
public:
    enum ParseResult {
        PR_OK,
        PR_FAIL,
        PR_SECTION_NOT_EXIST
    };
public:
    ClusterConfigParser(const std::string &basePath, 
                        const std::string &clusterFilePath);
    ~ClusterConfigParser();
private:
    ClusterConfigParser(const ClusterConfigParser &);
    ClusterConfigParser& operator=(const ClusterConfigParser &);
public:
    // get section config
    ParseResult getClusterConfigInfo(ClusterConfigInfo &clusterConfigInfo) const;
    ParseResult getFuncConfig(suez::turing::FuncConfig &funcConfig) const;
    ParseResult getAggSamplerConfigInfo(AggSamplerConfigInfo &aggSamplerInfo) const;
    ParseResult getFinalSorterConfig(FinalSorterConfig &finalSorterConfig) const;
    ParseResult getSearchOptimizerConfig(SearchOptimizerConfig &searchOptimizerConfig) const;
    ParseResult getRankProfileConfig(RankProfileConfig &rankProfileConfig) const;
    ParseResult getSummaryProfileConfig(
            SummaryProfileConfig &summaryProfileConfig) const;
    ParseResult getSearcherCacheConfig(
            SearcherCacheConfig &searcherCacheConfig) const;
    ParseResult getServiceDegradationConfig(
            ServiceDegradationConfig &serviceDegradationConfig) const;
    ParseResult getAnomalyProcessConfig(
            AnomalyProcessConfig &anomalyProcessConfig) const;
    ParseResult getAnomalyProcessConfigT(
            AnomalyProcessConfig &anomalyProcessConfig) const;
    ParseResult getSqlAnomalyProcessConfig(
            AnomalyProcessConfig &anomalyProcessConfig) const;
    ParseResult getCavaConfig(suez::turing::CavaConfig &cavaConfig) const;
    ParseResult getTuringOptionsInfo(TuringOptionsInfo &turingOptionsInfo) const;
private:
    bool parse(const std::string &jsonFile, 
               autil::legacy::json::JsonMap &jsonMap) const;
    bool merge(autil::legacy::json::JsonMap &jsonMap) const;
    bool mergeJsonMap(const autil::legacy::json::JsonMap &src,
                      autil::legacy::json::JsonMap &dst) const;
    bool mergeJsonArray(autil::legacy::json::JsonArray &dst) const;
    bool mergeWithInherit(autil::legacy::json::JsonMap &jsonMap) const;

    template <typename T>
    ParseResult doGetConfig(const std::string &configFile, 
                            const std::string &sectionName, 
                            T &config) const;
private:
    std::string _basePath;
    std::string _clusterFilePath;
    mutable std::set<uint64_t> _contentSignatures; 
    mutable std::vector<std::string> _configFileNames; // for inherit loop detection
private:
    AUTIL_LOG_DECLARE();
};

typedef std::shared_ptr<ClusterConfigParser> ClusterConfigParserPtr;
////////////////////////////////////////////////////

} // namespace config
} // namespace isearch

