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
#include "ha3/sql/ops/planOptimize/model/SqlModelGraphOptimizer.h"

#include <rapidjson/stringbuffer.h>
#include <algorithm>
#include <cstddef>
#include <unordered_map>
#include <utility>

#include "autil/legacy/any.h"
#include "autil/legacy/base64.h"
#include "autil/legacy/exception.h"
#include "autil/legacy/legacy_jsonizable.h"
#include "ha3/common/CommonDef.h"
#include "ha3/common/Tracer.h"
#include "ha3/service/QrsModelHandler.h"
#include "ha3/sql/common/KvPairParser.h"
#include "ha3/sql/common/common.h"
#include "ha3/turing/common/ModelConfig.h"
#include "iquan/jni/IquanDqlResponse.h"
#include "aios/network/gig/multi_call/common/ErrorCode.h"
#include "aios/network/gig/multi_call/common/common.h"

using namespace std;
using namespace suez::turing;
using namespace multi_call;
using namespace autil::legacy;

using namespace isearch::turing;
using namespace isearch::sql;
using namespace isearch::common;
using namespace isearch::service;

namespace isearch {
namespace sql {

AUTIL_LOG_SETUP(ha3, SqlModelGraphOptimizer);

bool SqlModelGraphOptimizer::optimize(iquan::SqlPlan &sqlPlan)
{
    vector<std::string> keys;
    vector<std::string> modelBizs;
    vector<map<string, string>> kvPairs;
    getModelBizs(sqlPlan, keys, modelBizs, kvPairs);
    vector<map<string, string>> result;
    if (!_qrsModelHandlerPtr->process(modelBizs, kvPairs, multi_call::INVALID_SOURCE_ID,
                    _modelConfigMap, _pool, _querySession, true, result))
    {
        SQL_LOG(WARN, "QrsModelHandler process failed");
        return false;
    }
    if (result.size() != keys.size()) {
        SQL_LOG(ERROR, "result size [%lu] not equal to keys size [%lu]",
                      result.size(), keys.size());
        return false;
    }
    for (size_t i = 0; i < result.size(); ++i) {
        sqlPlan.innerDynamicParams.addKVParams(keys[i], ToJsonString(result[i]));
    }
    if (_tracer) {
        SQL_LOG(DEBUG, "%s", _tracer->getTraceInfo().c_str());
    }
    return true;
}

void SqlModelGraphOptimizer::getModelBizs(iquan::SqlPlan &sqlPlan,
        vector<string> &keys,
        vector<string> &modelBizs,
        vector<map<string, string>> &kvPairs)
{
    for (const auto &pair: sqlPlan.optimizeInfosMap) {
        if (pair.first != "0") {
            continue;
        }
        for (const auto &optimizeInfos: pair.second) {
            const auto &optimizeInfo = optimizeInfos.optimizeInfo;
            if (optimizeInfo.type == SQL_UDF_OP) {
                if (optimizeInfo.op == SQL_UDF_AITHETA_OP) {
                    keys.push_back(optimizeInfos.key[0]);
                    modelBizs.emplace_back(
                            getParam(optimizeInfo.params[0], sqlPlan.innerDynamicParams));
                    map<string, string> kvPair;
                    KvPairParser::parse(
                            getParam(optimizeInfo.params[1], sqlPlan.innerDynamicParams),
                            SQL_MODEL_KV_PAIR_SPLIT,
                            SQL_MODEL_KV_SPLIT, kvPair);
                    auto iter = kvPair.find(QINFO_KEY_FG_USER);
                    if (iter != kvPair.end()) {
                        iter->second = autil::legacy::Base64DecodeFast(iter->second);
                    }
                    kvPairs.emplace_back(kvPair);
                }
            } else if (optimizeInfo.type == SQL_TVF_OP) {
                auto iter = _modelConfigMap->find(optimizeInfo.op);
                if (iter != _modelConfigMap->end() &&
                    iter->second.modelType == MODEL_TYPE::SCORE_MODEL)
                {
                    keys.emplace_back(optimizeInfos.key[0]);
                    modelBizs.emplace_back(optimizeInfo.op);
                    map<string, string> kvPair;
                    KvPairParser::parse(
                            getParam(optimizeInfo.params[0], sqlPlan.innerDynamicParams),
                            SQL_MODEL_KV_PAIR_SPLIT,
                            SQL_MODEL_KV_SPLIT, kvPair);
                    auto iter = kvPair.find(QINFO_KEY_FG_USER);
                    if (iter != kvPair.end()) {
                        iter->second = autil::legacy::Base64DecodeFast(iter->second);
                    }
                    kvPairs.emplace_back(kvPair);
                }
            }
        }
    }
}

string SqlModelGraphOptimizer::getParam(
        const std::string &param, iquan::DynamicParams &innerDynamicParams)
{
    rapidjson::StringBuffer buf;
    autil::legacy::RapidWriter writer(buf);
    iquan::PlanOp::PlanOpHandle planOpHandle(writer, &_dynamicParams, &innerDynamicParams);
    planOpHandle.String(param.c_str(), param.size());
    return string(buf.GetString() + 1, buf.GetSize() - 2);
}

} // namespace sql
} // namespace isearch
