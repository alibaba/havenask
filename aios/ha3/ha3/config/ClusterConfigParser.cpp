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
#include "ha3/config/ClusterConfigParser.h"

#include <algorithm>
#include <iosfwd>
#include <map>
#include <typeinfo>
#include <utility>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/any.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/json.h"
#include "autil/legacy/jsonizable.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/config/AggSamplerConfigInfo.h"
#include "ha3/config/AnomalyProcessConfig.h"
#include "ha3/config/ClusterConfigInfo.h"
#include "ha3/config/FinalSorterConfig.h"
#include "ha3/config/RankProfileConfig.h"
#include "ha3/config/SearchOptimizerConfig.h"
#include "ha3/config/SearcherCacheConfig.h"
#include "ha3/config/ServiceDegradationConfig.h"
#include "ha3/config/SummaryProfileConfig.h"
#include "ha3/config/TuringOptionsInfo.h"
#include "ha3/util/TypeDefine.h"
#include "indexlib/config/module_info.h"
#include "fslib/util/FileUtil.h"
#include "suez/turing/expression/cava/common/CavaConfig.h"
#include "suez/turing/expression/common.h"
#include "suez/turing/expression/function/FuncConfig.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace autil::legacy::json;
using namespace suez::turing;
using namespace fslib::util;
using namespace indexlib::config;

namespace isearch {
namespace config {
AUTIL_LOG_SETUP(ha3, ClusterConfigParser);

typedef ClusterConfigParser::ParseResult ParseResult;

ClusterConfigParser::ClusterConfigParser(const string &basePath,
        const string &clusterConfigFilePath)
{
    _basePath = basePath;
    _clusterFilePath = clusterConfigFilePath;
}

ClusterConfigParser::~ClusterConfigParser() {
}

bool ClusterConfigParser::parse(const string &jsonFile, JsonMap &jsonMap) const {
    string content;
    if (!FileUtil::readFile(jsonFile, content)) {
        AUTIL_LOG(ERROR, "read file %s failed", jsonFile.c_str());
        return false;
    }
    // check loop in an inheritance path
    if (find(_configFileNames.begin(), _configFileNames.end(), jsonFile)
        != _configFileNames.end()) {
        AUTIL_LOG(ERROR, "inherit loop detected, file: %s", jsonFile.c_str());
        return false;
    }
    _configFileNames.push_back(jsonFile);

    try {
        FromJsonString(jsonMap, content);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "jsonize failed, exception[%s] str[%s]", e.what(), content.c_str());
        return false;
    }
    if (!merge(jsonMap)) {
        AUTIL_LOG(ERROR, "merge inherit of  %s failed",
                content.c_str());
        return false;
    }

    // NOTE: it won't pop_back in "return false" code path,
    // since it already fails ...
    _configFileNames.pop_back();

    return true;
}

bool ClusterConfigParser::merge(JsonMap &jsonMap) const {
    // merge with inner level
    // inner level takes effect first, then outter level fill the blanks
    // that inner level left.
    for (JsonMap::iterator it = jsonMap.begin();
         it != jsonMap.end(); it++)
    {
        const string &key = it->first;
        Any &any = it->second;
        if (any.GetType() == typeid(JsonMap)) {
            JsonMap &childJsonMap = *AnyCast<JsonMap>(&any);
            if (!merge(childJsonMap)) {
                AUTIL_LOG(ERROR, "merge child %s fail",
                        key.c_str());
                return false;
            }
        }
    }
    // merge with this level
    if (!mergeWithInherit(jsonMap)) {
        AUTIL_LOG(ERROR, "merge with inherit fail");
        return false;
    }
    return true;
}

bool ClusterConfigParser::mergeWithInherit(JsonMap &jsonMap) const {
    JsonMap::iterator it = jsonMap.find(INHERIT_FROM_SECTION);
    if (it == jsonMap.end()) {
        return true;
    }
    string inheritFromFile;
    try {
        FromJson(inheritFromFile, it->second);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "jsonize inherit_from failed, exception: %s", e.what());
        return false;
    }
    inheritFromFile = FileUtil::joinFilePath(_basePath, inheritFromFile);
    JsonMap inheritJsonMap;
    if (!parse(inheritFromFile, inheritJsonMap)) {
        AUTIL_LOG(ERROR, "parse %s failed", inheritFromFile.c_str());
        return false;
    }
    return mergeJsonMap(inheritJsonMap, jsonMap);
}

bool ClusterConfigParser::mergeJsonMap(const JsonMap &src, JsonMap &dst) const {
    JsonMap::const_iterator it = src.begin();
    for (; it != src.end(); it++) {
        const string &key = it->first;
        const Any &value = it->second;
        if (dst.find(key) == dst.end()) {
            dst[key] = value;
        } else if (dst[key].GetType() != value.GetType()) {
            AUTIL_LOG(ERROR, "merge error, type not match for %s", key.c_str());
            return false;
        } else if (value.GetType() == typeid(JsonMap)) {
            JsonMap &dstChild = *AnyCast<JsonMap>(&dst[key]);
            const JsonMap &srcChild = AnyCast<JsonMap>(value);
            if (!mergeJsonMap(srcChild, dstChild)) {
                AUTIL_LOG(ERROR, "merge child[%s] failed", key.c_str());
                return false;
            }
        } else if (value.GetType() == typeid(JsonArray)) {
            if (!mergeJsonArray(*AnyCast<JsonArray>(&dst[key]))) {
                AUTIL_LOG(ERROR, "merge child[%s] failed", key.c_str());
                return false;
            }
        }
    }
    return true;
}

bool ClusterConfigParser::mergeJsonArray(JsonArray &dst) const {
    for (JsonArray::iterator it = dst.begin(); it != dst.end(); it++) {
        if (it->GetType() == typeid(JsonMap)) {
            if (!merge(*AnyCast<JsonMap>(&*it))) {
                AUTIL_LOG(ERROR, "merge child[%s] failed", ToString(*it).c_str());
                return false;
            }
        } else if (it->GetType() == typeid(JsonArray)) {
            if (!mergeJsonArray(*AnyCast<JsonArray>(&*it))) {
                AUTIL_LOG(ERROR, "merge child[%s] failed", ToString(*it).c_str());
                return false;
            }
        }
    }
    return true;
}

#define DECLARE_GET_CONFIG_FUNCTION(configType, sectionName)            \
    ParseResult ClusterConfigParser::get##configType(configType &config) const \
    {                                                                   \
        return doGetConfig(_clusterFilePath, sectionName,config);       \
    }
DECLARE_GET_CONFIG_FUNCTION(ClusterConfigInfo, CLUSTER_CONFIG_INFO_SECTION)
DECLARE_GET_CONFIG_FUNCTION(FuncConfig, FUNCTION_CONFIG_SECTION)
DECLARE_GET_CONFIG_FUNCTION(AggSamplerConfigInfo, AGGREGATE_SAMPLER_CONFIG_SECTION)
DECLARE_GET_CONFIG_FUNCTION(FinalSorterConfig, FINAL_SORTER_CONFIG_SECTION)
DECLARE_GET_CONFIG_FUNCTION(SearchOptimizerConfig, SEARCH_OPTIMIZER_CONFIG_SECTION)
DECLARE_GET_CONFIG_FUNCTION(RankProfileConfig, RANK_PROFILE_SECTION)
DECLARE_GET_CONFIG_FUNCTION(SummaryProfileConfig, SUMMARY_CONFIG_SECTION)
DECLARE_GET_CONFIG_FUNCTION(SearcherCacheConfig, SEARCHER_CACHE_CONFIG_SECTION)
DECLARE_GET_CONFIG_FUNCTION(ServiceDegradationConfig, SERVICE_DEGRADATION_CONFIG_SECTION)
DECLARE_GET_CONFIG_FUNCTION(AnomalyProcessConfig, ANOMALY_PROCESS_CONFIG_SECTION)
DECLARE_GET_CONFIG_FUNCTION(AnomalyProcessConfigT, ANOMALY_PROCESS_CONFIG_SECTION_T)
DECLARE_GET_CONFIG_FUNCTION(SqlAnomalyProcessConfig, ANOMALY_PROCESS_CONFIG_SECTION_SQL)
DECLARE_GET_CONFIG_FUNCTION(TuringOptionsInfo, TURING_OPTIONS_SECTION)
#undef DECLARE_GET_CONFIG_FUNCTION

ParseResult ClusterConfigParser::getCavaConfig(CavaConfig &config) const
{
    config._cavaConf = HA3_CAVA_CONF;// important!
    return doGetConfig(_clusterFilePath, CAVA_CONFIG_SECTION,config);
}

template <typename T>
ParseResult ClusterConfigParser::doGetConfig(
        const string &configFile,
        const string &sectionName,
        T &config) const
{
    _configFileNames.clear();
    JsonMap jsonMap;
    if (!parse(configFile, jsonMap)) {
        AUTIL_LOG(ERROR, "failed to parse %s", configFile.c_str());
        return PR_FAIL;
    }
    JsonMap::iterator it = jsonMap.find(sectionName);
    if (it == jsonMap.end()) {
        return PR_SECTION_NOT_EXIST;
    }
    try {
        FromJson(config, it->second);
    } catch (const ExceptionBase &e) {
        AUTIL_LOG(ERROR, "parse section %s failed, exception: %s",
                sectionName.c_str(), e.what());
        return PR_FAIL;
    }
    return PR_OK;
}





} // namespace config
} // namespace isearch
