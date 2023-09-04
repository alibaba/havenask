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
#include "suez/worker/KMonitorMetaParser.h"

#include <cstddef>
#include <ext/alloc_traits.h>
#include <map>

#include "alog/Logger.h"
#include "autil/Log.h"
#include "autil/StringUtil.h"
#include "suez/sdk/KMonitorMetaInfo.h"

using namespace std;
using namespace autil;

namespace suez {

AUTIL_DECLARE_AND_SETUP_LOGGER(suez, KMonitorMetaParser);

static const string SERVICE_SEP = ".";
static const string AMONITOR_SEP = "/";
static const string KEYVALUE_SEP = "^";
static const string MULTI_SEP = "@";

static const string DEFAULT_METRIC_PREFIX = "tisplus.amon.";
static const string DEFAULT_METRIC_SUFFIX = ".ha3suez";
static const string QRS_SUFFIX = ".qrs";
static const string SEARCHER_SUFFIX = ".searcher";
static const string ROLE_TYPE_QRS = "qrs";
static const string ROLE_TYPE_SEARCHER = "searcher";

static constexpr size_t TISPLUS_PATTERN_SIZE = 3;
static constexpr size_t DEFAULT_PATTERN_SIZE = 1;

bool KMonitorMetaParser::parse(const KMonitorMetaParam &param, KMonitorMetaInfo &result) {
    const auto &amonTagVec = StringUtil::split(param.amonitorPath, AMONITOR_SEP);
    if (amonTagVec.size() >= 1) {
        result.tagsMap["instance"] = amonTagVec[0];
    }
    if (amonTagVec.size() >= 2) {
        result.tagsMap["zone"] = amonTagVec[1];
    }
    result.tagsMap["role"] = param.partId;

    vector<string> patternVec = StringUtil::split(param.serviceName, SERVICE_SEP);
    if (patternVec.size() == TISPLUS_PATTERN_SIZE) {
        return parseTisplus(patternVec, param.roleType, result);
    } else if (patternVec.size() == DEFAULT_PATTERN_SIZE) {
        return parseDefault(param.serviceName, param.roleType, result);
    } else {
        AUTIL_LOG(WARN, "parse pattern failed, str [%s], size is [%lu].", param.serviceName.c_str(), patternVec.size());
        return false;
    }
    return true;
}

bool KMonitorMetaParser::parseDefault(const string &serviceName, string roleType, KMonitorMetaInfo &result) {
    result.metricsPrefix = DEFAULT_METRIC_PREFIX + serviceName + DEFAULT_METRIC_SUFFIX;
    result.serviceName = serviceName;
    if (roleType == ROLE_TYPE_QRS) {
        result.metricsPrefix += QRS_SUFFIX;
    } else if (roleType == ROLE_TYPE_SEARCHER) {
        result.metricsPrefix += SEARCHER_SUFFIX;
    }
    return true;
}

bool KMonitorMetaParser::parseTisplus(const vector<string> &patternVec, string roleType, KMonitorMetaInfo &result) {
    if (patternVec.size() < 3) {
        return false;
    }
    result.metricsPrefix = patternVec[0] + DEFAULT_METRIC_SUFFIX;
    result.serviceName = patternVec[0];
    if (roleType == ROLE_TYPE_QRS) {
        result.metricsPrefix += QRS_SUFFIX;
    } else if (roleType == ROLE_TYPE_SEARCHER) {
        result.metricsPrefix += SEARCHER_SUFFIX;
    }
    result.tagsMap["app"] = patternVec[1];
    result.tagsMap["service_type"] = "online";
    auto tagVec = StringUtil::split(patternVec[2], MULTI_SEP);
    for (const auto &tagStr : tagVec) {
        auto keyVec = StringUtil::split(tagStr, KEYVALUE_SEP);
        if (keyVec.size() != 2) {
            AUTIL_LOG(ERROR, "parse tisplus kmon tag [%s] failed.", tagStr.c_str());
            return false;
        }
        result.tagsMap[keyVec[0]] = keyVec[1];
    }
    return true;
}

} // namespace suez
