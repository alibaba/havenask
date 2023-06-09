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
#include "indexlib_plugin/plugins/aitheta/common_define.h"
#include <aitheta/index_distance.h>
#include "indexlib/util/KeyValueMap.h"
#include "indexlib_plugin/plugins/aitheta/util/param_util.h"
#include "indexlib_plugin/plugins/aitheta/util/indexlib_io_wrapper.h"

using namespace std;
using namespace aitheta;
using namespace autil::legacy;
using namespace indexlib::util;
using namespace indexlib::file_system;
using namespace indexlib::file_system;

namespace indexlib {
namespace aitheta_plugin {
IE_LOG_SETUP(aitheta_plugin, SchemaParameter);

SchemaParameter::SchemaParameter(const util::KeyValueMap &parameters)
    : indexType("unknown"),
      dimension(INVALID_DIMENSION),
      knnLrThold(0ul),
      searchDistType(DistType::kUnknown),
      buildDistType(DistType::kUnknown),
      forceBuildIndex(false),
      topkKey("&n="),
      sfKey("&sf="),
      catidEmbedSeparator("#"),
      catidSeparator(","),
      embedSeparator(","),
      querySeparator(";") {
    keyValMap = parameters;

    indexType = GetValueFromKeyValueMap(parameters, INDEX_TYPE, indexType);

    const string sDistTyStr = GetValueFromKeyValueMap(parameters, SEARCH_METRIC_TYPE);
    if (!Str2Type(sDistTyStr, searchDistType)) {
        IE_LOG(ERROR, "failed to get search dist-type from params");
    }

    const string bDistTyStr = GetValueFromKeyValueMap(parameters, BUILD_METRIC_TYPE);
    if (!Str2Type(bDistTyStr, buildDistType)) {
        IE_LOG(WARN, "failed to get build dist-type from params");
    }

    ParamUtil::ExtractValue(parameters, DIMENSION, &dimension);
    ParamUtil::ExtractValue(parameters, USE_LINEAR_THRESHOLD, &knnLrThold);
    ParamUtil::ExtractValue(parameters, METRIC_RECALL_ENABLE, &metricOption.enableRecReport);
    ParamUtil::ExtractValue(parameters, METRIC_RECALL_SAMPLE_INTERVAL, &metricOption.recReportSampleInterval);
    ParamUtil::ExtractValue(parameters, GPU_SEARCH_DOC_NUM_LOW_BOUND, &gpuSearchOption.docNumLowBound);
    const string deviceTy = GetValueFromKeyValueMap(parameters, SEARCH_DEVICE_TYPE);
    gpuSearchOption.enable = (0 == strcasecmp(deviceTy.c_str(), DEVICE_TYPE_GPU.c_str()) ? true : false);
    ParamUtil::ExtractValue(parameters, TOPK_ADJUST_ZOOMOUT, &topkOption.zoomout);
    bool isTopkAdjEconomic = false;
    ParamUtil::ExtractValue(parameters, TOPK_ADJUST_ECONOMIC, &isTopkAdjEconomic);
    if (isTopkAdjEconomic) {
        topkOption.plan = TopkOption::Plan::kEconomic;
    }

    ParamUtil::ExtractValue(parameters, RT_ENABLE, &rtOption.enable);
    ParamUtil::ExtractValue(parameters, RT_SKIP_DOC, &rtOption.skipDoc);
    ParamUtil::ExtractValue(parameters, RT_QUEUE_SIZE, &rtOption.queueSize);
    ParamUtil::ExtractValue(parameters, RT_MIPS_NORM, &rtOption.mipsNorm);
    ParamUtil::ExtractValue(parameters, RT_DOC_NUM_PREDICT, &rtOption.docNumPredict);
    if (rtOption.enable) {
        incOption.enable = true;
        IE_LOG(INFO, "inc build is forcely enabled when rt build enabled");
    } else {
        ParamUtil::ExtractValue(parameters, INC_ENABLE, &incOption.enable);
    }

    ParamUtil::ExtractValue(parameters, FORCE_BUILD_INDEX, &forceBuildIndex);
    topkKey = GetValueFromKeyValueMap(parameters, TOPK_KEY, topkKey);
    sfKey = GetValueFromKeyValueMap(parameters, SCORE_FILTER_KEY, sfKey);
    catidSeparator = GetValueFromKeyValueMap(parameters, CATEGORY_SEPARATOR, catidSeparator);
    embedSeparator = GetValueFromKeyValueMap(parameters, EMBEDDING_SEPARATOR, embedSeparator);
    querySeparator = GetValueFromKeyValueMap(parameters, QUERY_SEPARATOR, querySeparator);
    catidEmbedSeparator = GetValueFromKeyValueMap(parameters, CATEGORY_EMBEDDING_SEPARATOR, catidEmbedSeparator);
}

void MipsParams::Jsonize(JsonWrapper &json) {
    json.Jsonize("enable", enable, false);
    json.Jsonize("mval", mval, 0U);
    json.Jsonize("uval", uval, 0.0f);
    json.Jsonize("norm", norm, 0.0f);
}

std::string Query::toString() const {
    std::stringstream ss;
    ss << "catid: [" << catid << "], ";
    ss << "topk: [" << topk << "], ";
    ss << "enableScoreFilter: [" << enableScoreFilter << "], ";
    ss << "threshold: [" << threshold << "], ";
    ss << "dimension: [" << dimension << "], ";
    ss << "queryNum: [" << queryNum << "], ";
    ss << "], embedding : [";
    for (uint32_t i = 0; i < dimension * queryNum; ++i) {
        ss << embedding.get()[i];
        if (i + 1 == dimension * queryNum) {
            ss << "]";
        } else {
            ss << ",";
        }
    }

    return ss.str();
}

bool Str2Type(const std::string &str, DistType &ty) {
    if (L2 == str) {
        ty = DistType::kL2;
        return true;
    }
    if (IP == str) {
        ty = DistType::kIP;
        return true;
    }
    ty = DistType::kUnknown;
    return false;
}

std::string Type2Str(const SegmentType &ty) {
    static const std::vector<std::string> kType = {"unknown", "raw", "index", "parallelMegreIndex", "realtimeIndex"};
    return kType[static_cast<int16_t>(ty)];
}

}
}
