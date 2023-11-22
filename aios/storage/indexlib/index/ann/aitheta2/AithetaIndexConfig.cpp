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
#include "indexlib/index/ann/aitheta2/AithetaIndexConfig.h"

#include <stdlib.h>

#include "autil/legacy/jsonizable.h"
#include "indexlib/index/ann/aitheta2/util/ParamUtil.h"

using namespace std;
using namespace autil::legacy;
using namespace autil::legacy::json;
namespace indexlibv2::index::ann {
static const string ENV_PARAMS_PREFIX = "INDEXLIB_PLUGIN_PARAM_";
static const string CUSTOMIZED_AITHETA_PARAMS = "aitheta_params";

void AithetaBuildConfig::Parse(const indexlib::util::KeyValueMap& parameters)
{
    ParamUtil::ExtractValue(parameters, INDEX_BUILDER_NAME, &builderName);
    ParamUtil::ExtractValue(parameters, INDEX_BUILD_IN_FULL_BUILD_PHASE, &buildInFullBuildPhase);
    ParamUtil::ExtractValue(parameters, INDEX_BUILD_THRESHOLD, &buildThreshold);
    ParamUtil::ExtractValue(parameters, STORE_PRIMARY_KEY, &storePrimaryKey);
    ParamUtil::ExtractValue(parameters, STORE_ORIGINAL_EMBEDDING, &storeEmbedding);
    ParamUtil::ExtractValue(parameters, IGNORE_FIELD_COUNT_MISMATCH, &ignoreFieldCountMismatch);
    ParamUtil::ExtractValue(parameters, IGNORE_INVALID_DOC, &ignoreInvalidDoc);
    string key =
        parameters.find(INDEX_BUILD_PARAMETERS) != parameters.end() ? INDEX_BUILD_PARAMETERS : INDEX_PARAMETERS;
    ParamUtil::ExtractValue(parameters, key, &indexParams);
    ParamUtil::ExtractValue(parameters, DISTRIBUTED_BUILD, &distributedBuild);
    ParamUtil::ExtractValue(parameters, CENTROID_COUNT, &centroidCount);
    ParamUtil::ExtractValue(parameters, PARALLEL_NUM, &parallelNum);
}

void AithetaSearchConfig::Parse(const indexlib::util::KeyValueMap& parameters)
{
    ParamUtil::ExtractValue(parameters, INDEX_SEARCHER_NAME, &searcherName);
    ParamUtil::ExtractValue(parameters, INDEX_SCAN_COUNT, &scanCount);
    string key =
        parameters.find(INDEX_SEARCH_PARAMETERS) != parameters.end() ? INDEX_SEARCH_PARAMETERS : INDEX_PARAMETERS;
    ParamUtil::ExtractValue(parameters, key, &indexParams);
}

void AithetaRealtimeConfig::Parse(const indexlib::util::KeyValueMap& parameters)
{
    ParamUtil::ExtractValue(parameters, REALTIME_PARAMETERS, &indexParams);
    ParamUtil::ExtractValue(parameters, STORE_PRIMARY_KEY, &storePrimaryKey);
    ParamUtil::ExtractValue(parameters, INDEX_STREAMER_NAME, &streamerName);
    ParamUtil::ExtractValue(parameters, REALTIME_ENABLE, &enable);
    if (!streamerName.empty()) {
        enable = true;
    }
}

void AithetaRecallConfig::Parse(const indexlib::util::KeyValueMap& parameters)
{
    ParamUtil::ExtractValue(parameters, RECALL_ENABLE, &enable);
    ParamUtil::ExtractValue(parameters, RECALL_SAMPLE_RATIO, &sampleRatio);
}

AithetaIndexConfig::AithetaIndexConfig(const indexlib::util::KeyValueMap& parameters) { Parse(parameters); }

void AithetaIndexConfig::Parse(const indexlib::util::KeyValueMap& parameters)
{
    ParamUtil::ExtractValue(parameters, DIMENSION, &dimension);
    ParamUtil::ExtractValue(parameters, DISTANCE_TYPE, &distanceType);
    ParamUtil::ExtractValue(parameters, EMBEDDING_DELIMITER, &embeddingDelimiter);

    buildConfig.Parse(parameters);
    searchConfig.Parse(parameters);
    realtimeConfig.Parse(parameters);
    recallConfig.Parse(parameters);
}

bool AithetaIndexConfig::ModifyConfig(const string& indexName, const indexlib::util::KeyValueMap& parameters)
{
    auto it = parameters.find(CUSTOMIZED_AITHETA_PARAMS);
    if (it == parameters.end()) {
        return true;
    }

    AUTIL_LOG(INFO, "[%s] found in customized parameters", CUSTOMIZED_AITHETA_PARAMS.c_str());
    JsonMap jsonMap;
    try {
        FromJsonString(jsonMap, it->second);
    } catch (...) {
        AUTIL_LOG(ERROR, "jsonize[%s] failed in build config", it->second.c_str());
        return false;
    }

    auto indexIt = jsonMap.find(indexName);
    if (indexIt == jsonMap.end()) {
        AUTIL_LOG(INFO, "index[%s] not found in customized parameters", indexName.c_str());
        return true;
    }

    AUTIL_LOG(INFO, "update index[%s] with customized parameter[%s]", indexName.c_str(),
              ToJsonString(indexIt->second, true).c_str());

    indexlib::util::KeyValueMap params;
    try {
        map<string, Any> paramMap = AnyCast<JsonMap>(indexIt->second);
        map<string, Any>::iterator paramIter = paramMap.begin();
        for (; paramIter != paramMap.end(); paramIter++) {
            string value;
            if (paramIter->second.GetType() == typeid(string)) {
                value = AnyCast<string>(paramIter->second);
            } else {
                json::ToString(paramIter->second, value, true, "");
            }
            params.insert(make_pair(paramIter->first, value));
        }
    } catch (autil::legacy::BadAnyCast& e) {
        AUTIL_LOG(ERROR, "bad any cast");
        return false;
    } catch (...) {
        AUTIL_LOG(ERROR, "ModifyConfig caught exception");
        return false;
    }

    Parse(params);
    return true;
}

AUTIL_LOG_SETUP(indexlib.index, AithetaIndexConfig);

} // namespace indexlibv2::index::ann
