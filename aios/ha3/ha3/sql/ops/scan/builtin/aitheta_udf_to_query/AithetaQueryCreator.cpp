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
#include "ha3/sql/ops/scan/builtin/AithetaQueryCreator.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>

#include "alog/Logger.h"
#include "autil/CommonMacros.h"
#include "autil/StringUtil.h"
#include "autil/TimeUtility.h"
#include "autil/legacy/base64.h"
#include "ha3/common/Query.h"
#include "ha3/common/Term.h"
#include "ha3/common/TermQuery.h"
#include "ha3/isearch.h"
#include "ha3/sql/common/Log.h"
#include "tensorflow/core/framework/tensor.h"
#include "tensorflow/core/framework/tensor.pb.h"
#include "tensorflow/core/framework/types.h"
#include "tensorflow/core/framework/types.pb.h"
#include "tensorflow/core/platform/types.h"
#include "turing_ops_util/variant/Tracer.h"

using namespace std;
using namespace autil;
using namespace tensorflow;

using namespace isearch::common;
using namespace isearch::turing;
using namespace suez::turing;

namespace isearch {
namespace sql {
AUTIL_LOG_SETUP(sql, AithetaQueryCreator);

static const std::string MULTI_VALUE_SEPARATOR_STR = "";

Query *AithetaQueryCreator::createQuery(const std::string &modelBiz,
                                        const std::map<std::string, std::string> &kvPairs,
                                        const std::map<std::string, std::string> &embedding,
                                        ModelConfigMap *modelConfigMap) {
    if (!modelConfigMap) {
        SQL_LOG(ERROR, "get modelConfigMap failed");
        return nullptr;
    }
    auto iter = modelConfigMap->find(modelBiz);
    if (iter == modelConfigMap->end()) {
        SQL_LOG(ERROR, "not find model biz [%s] meta", modelBiz.c_str());
        return nullptr;
    }
    const ModelConfig &modelConfig = iter->second;
    vector<Query *> allQuery;
    RequiredFields requiredFields;
    for (const auto &node : modelConfig.searcherModelInfo.index) {
        auto pair = embedding.find(node.user);
        if (pair != embedding.end()) {
            vector<string> tensorStrs;
            TensorProto tensorProto;
            if (!tensorProto.ParseFromString(autil::legacy::Base64DecodeFast(pair->second))) {
                SQL_LOG(ERROR, "parse tensor from kvpair [%s] failed", pair->second.c_str());
                reset(allQuery);
                return nullptr;
            }
            if (!genAithetaQuery(tensorProto, tensorStrs)) {
                SQL_LOG(TRACE1, "gen aitheta query failed");
                reset(allQuery);
                return nullptr;
            }
            const vector<string> &queryStrs = makeQuery(kvPairs, modelBiz, tensorStrs);
            for (auto &queryStr : queryStrs) {
                allQuery.push_back(new TermQuery(queryStr, node.item, requiredFields, node.label));
            }
        } else {
            SQL_LOG(ERROR,
                    "not find model biz [%s] user node [%s] in kvpairs",
                    modelBiz.c_str(),
                    node.user.c_str());
            reset(allQuery);
            return nullptr;
        }
    }

    if (allQuery.empty()) {
        SQL_LOG(ERROR, "empty aitheta query");
        return nullptr;
    }
    if (allQuery.size() == 1) {
        return allQuery[0];
    }
    Query *orQuery = Query::createQuery(OR_QUERY);
    for (Query *q : allQuery) {
        orQuery->addQuery(QueryPtr(q));
    }
    return orQuery;
}

void AithetaQueryCreator::reset(vector<Query *> &allQuery) {
    for (size_t i = 0; i < allQuery.size(); ++i) {
        DELETE_AND_SET_NULL(allQuery[i]);
    }
}

vector<string> AithetaQueryCreator::makeQuery(const map<string, string> &kvPairs,
                                              const string &modelBiz,
                                              const vector<string> &tensorStrs) {
    string category;
    auto categoryPair = kvPairs.find("category");
    if (categoryPair != kvPairs.end()) {
        category = categoryPair->second + "#";
    }
    // no category no '#'
    string topK("100");
    auto topKPair = kvPairs.find("topk");
    if (topKPair != kvPairs.end()) {
        topK = topKPair->second;
    }
    string sf;
    auto sfPair = kvPairs.find("sf");
    if (sfPair != kvPairs.end()) {
        sf = sfPair->second;
    }
    vector<string> queryStrs;
    for (auto &tensorStr : tensorStrs) {
        if (tensorStr.empty()) {
            continue;
        }
        string queryStr = category + tensorStr + "&n=" + topK;
        if (!sf.empty()) {
            queryStr += "&sf=" + sf;
        }
        queryStrs.push_back(queryStr);
    }
    return queryStrs;
}

bool AithetaQueryCreator::genAithetaQuery(const tensorflow::TensorProto &tensorProto,
                                          vector<string> &tensorStrs,
                                          Tracer *tracer) {
    Tensor tensor;
    if (!tensor.FromProto(tensorProto)) {
        std::string errorMsg("tensor FromProto failed");
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR, "%s", errorMsg.c_str());
        SQL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    int dims = tensor.dims();
    int embSize = tensor.dim_size(dims - 1);
    int embNums = 1;
    for (int idx = 0; idx < dims - 1; ++idx) {
        embNums *= tensor.dim_size(idx);
    }

#define CASE_MACRO(label, T)                                                                       \
    case label: {                                                                                  \
        auto *pdata = tensor.base<T>();                                                            \
        for (int idx = 0; idx < embNums; ++idx) {                                                  \
            vector<string> tensorStr;                                                              \
            vector<T> tensorVec(pdata + idx * embSize, pdata + (idx + 1) * embSize);               \
            tensorStrs.push_back(autil::StringUtil::toString(tensorVec, ""));                     \
        }                                                                                          \
        break;                                                                                     \
    }

    switch (tensor.dtype()) {
        CASE_MACRO(DT_STRING, string);
        CASE_MACRO(DT_INT32, int32_t);
        CASE_MACRO(DT_INT64, int64);
        CASE_MACRO(DT_DOUBLE, double);
        CASE_MACRO(DT_FLOAT, float);
    default: {
        std::string errorMsg = "not support data type: " + DataTypeString(tensor.dtype());
        REQUEST_TRACE_WITH_TRACER(tracer, ERROR, "%s", errorMsg.c_str());
        SQL_LOG(ERROR, "%s", errorMsg.c_str());
        return false;
    }
    }
#undef CASE_MACRO
    return true;
}

} // namespace sql
} // namespace isearch
