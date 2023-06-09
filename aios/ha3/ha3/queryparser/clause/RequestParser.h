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

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <utility>
#include <vector>

#include "autil/HashFunctionBase.h"
#include "autil/StringTokenizer.h"
#include "autil/StringUtil.h"

#include "ha3/common/ErrorResult.h"
#include "ha3/common/FilterClause.h"
#include "ha3/common/Request.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/isearch.h"
#include "autil/Log.h" // IWYU pragma: keep

namespace isearch {
namespace common {
class ConfigClause;
class GlobalIdentifier;
struct RankHint;
}  // namespace common
namespace common {
class QueryInfo;
}  // namespace config
}  // namespace isearch

namespace isearch {
namespace queryparser {

class RequestParser
{
public:
    RequestParser();
    ~RequestParser();

public:
    bool parseRequest(common::RequestPtr &requestPtr,
                      const config::ClusterConfigMapPtr &clusterConfigMapPtr);

    bool parseConfigClause(common::RequestPtr &requestPtr);
    bool parseQueryClause(common::RequestPtr &requestPtr,
                          const common::QueryInfo &queryInfo);
    bool parseRankClause(common::RequestPtr &requestPtr);
    bool parseSortClause(common::RequestPtr &requestPtr);
    bool parseAggregateClause(common::RequestPtr &requestPtr);
    bool parseAggregateClausePtr(common::RequestPtr &requestPtr) {
        return parseAggregateClause(requestPtr);
    }
    bool parseFilterClause(common::FilterClause *filterClause);
    bool parseDistinctClause(common::RequestPtr &requestPtr);
    bool parseClusterClause(common::RequestPtr &requestPtr,
                            const config::ClusterConfigMapPtr &clusterConfigMapPtr);
    bool parseHealthCheckClause(common::RequestPtr &requestPtr);
    bool parseAttributeClause(common::RequestPtr &requestPtr);
    bool parseVirtualAttributeClause(common::RequestPtr &requestPtr);
    bool parseClause(common::RequestPtr &requestPtr);
    common::ErrorResult getErrorResult() {return _errorResult;}
    bool validateOriginalRequest(common::RequestPtr &requestPtr);
    bool parseFetchSummaryClause(common::RequestPtr &requestPtr);
    bool parseKVPairClause(common::RequestPtr &requestPtr);
    bool parseQueryLayerClause(common::RequestPtr &request);
    bool parseSearcherCacheClause(common::RequestPtr &request);
    bool parseOptimizerClause(common::RequestPtr &request);
    bool parseRankSortClause(common::RequestPtr &requestPtr);
    bool parseFinalSortClause(common::RequestPtr &requestPtr);
    bool parseLevelClause(common::RequestPtr &requestPtr);
    bool parseAnalyzerClause(common::RequestPtr &requestPtr);
    bool parseAuxQueryClause(common::RequestPtr &requestPtr);
    bool parseAuxFilterClause(common::AuxFilterClause *auxFilterClause);
public:
    static bool parseRankHint(const std::string &rankHintStr, common::RankHint &rankHint);
private:
    bool parseConfigClauseClusterNameVector(common::ConfigClause* configClause,
            const std::string& clusterNameVectorStr);
    bool parseConfigClauseKVPairs(common::ConfigClause* configClause,
                                  const std::string& oriStr);
    bool parseRankFieldBoostConfig(const std::string& oriStr,
                                   std::string& packageIndexName,
                                   std::string& fieldName, int32_t& fieldBoost);
    bool parseHashField(const autil::StringTokenizer::TokenVector &tokens,
                        const autil::HashFunctionBasePtr& hashFunc,
                        std::vector<std::pair<hashid_t, hashid_t> > &partIds);
    bool parsePartitionIds(const autil::StringTokenizer::TokenVector &tokens,
                           const autil::HashFunctionBasePtr& hashFunc,
                           std::vector<std::pair<hashid_t, hashid_t> > &partIds);
    bool parseTargetPartitions(
            const config::ClusterConfigInfo& clusterConfigInfo,
            const std::string& oriPartStr,
            std::vector<std::pair<hashid_t, hashid_t> >& targetParts);
    bool parseGid(const std::string &gidStr, std::string &clusterName,
                  common::GlobalIdentifier &gid);
    template<typename T>
    inline bool getValueFromGid(const std::string& gidStr,
                                const std::string& sep, size_t &begin,
                                T &value);
    bool getValueStrFromGid(const std::string& gidStr, const std::string& sep,
                            size_t &begin, std::string &value);
    bool parseRawPk(const std::string &str, size_t &pos,
                    std::string &clusterName,
                    std::vector<std::string> &rawPkVec);

    bool parseConfigClauseProtoFormatOption(common::ConfigClause* configClause,
            const std::string &value);
    bool parseSpecificIndexAnalyzerSection(const std::string& originalStr,
            common::ConfigClause *configClause);
    bool parseNoTokenizeIndexSection(const std::string& originalStr,
            common::ConfigClause *configClause);
    static std::string getNextTerm(const std::string &inputStr, char sep, size_t &start);
private:
    common::ErrorResult _errorResult;
private:
    friend class RequestParserTest;
private:
    AUTIL_LOG_DECLARE();
};

template<typename T>
inline bool RequestParser::getValueFromGid(const std::string& gidStr,
        const std::string& sep, size_t &begin, T &value)
{
    std::string valueStr;
    if (!getValueStrFromGid(gidStr, sep, begin, valueStr)) {
        return false;
    }
    value = autil::StringUtil::fromString<T>(valueStr);
    if (valueStr != autil::StringUtil::toString(value)) {
        return false;
    }

    return true;
}

template <>
inline bool RequestParser::getValueFromGid<primarykey_t>(const std::string& gidStr,
        const std::string& sep, size_t &begin, primarykey_t &value)
{
    std::string valueStr;
    if (!getValueStrFromGid(gidStr, sep, begin, valueStr)) {
        return false;
    }
    size_t valueHexSize = 2 * value.size();
    if (valueStr.length() > valueHexSize) {
        return false;
    }
    if (valueStr.length() < valueHexSize) {
        std::string prefix = std::string(valueHexSize - valueStr.length(), '0');
        valueStr = prefix + valueStr;
    }
    value = autil::StringUtil::fromString<primarykey_t>(valueStr);
    if (valueStr != autil::StringUtil::toString(value)) {
        return false;
    }

    return true;
}

} // namespace queryparser
} // namespace isearch
