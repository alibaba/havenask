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
/**
 * @file   knn_params_selector.cpp
 * @author luoli.hn <luoli.hn@taobao.com>
 * @date   Wed Jan 16 17:44:37 2019
 *
 * @brief
 *
 *
 */

#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/graph_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/param_util.h"
#include <aitheta/algorithm/mips_reformer.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, GraphKnnParamsSelector);
GraphKnnParamsSelector::GraphKnnParamsSelector(const KnnStrategies &strategies)
    : NoneLinearKnnParamsSelector(strategies) {}

GraphKnnParamsSelector::~GraphKnnParamsSelector() {}

bool GraphKnnParamsSelector::InitBuilderParams(const indexlib::util::KeyValueMap &parameters,
                                               aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docNum = 0;
    ParamUtil::ExtractValue(parameters, DOC_NUM, &docNum);
    indexlib::util::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docNum, dynamicParams)) {
        IE_LOG(DEBUG, "donnot need use dynamic params");
    }

    // 将parameters 与 动态参数合并
    ParamUtil::MergeParams(parameters, dynamicParams, true);

    string graphType = "hnsw";
    uint64_t graphMaxScanNum = 25000;
    uint64_t graphMaxDocCnt = 1000 * 1000 * 50;
    uint64_t graphNeighborCnt = 100;
    uint64_t memoryQuota = 128L * 1024L * 1024L * 1024L;
    uint64_t hnswEfconstruction = 400;
    uint64_t hnswMaxLevel = 6;
    uint64_t hnswScalingFactor = 50;
    uint64_t hnswUpperNeighborCnt = 50;
    bool graphWithQuantizer = false;
    uint64_t hnswEf = 200;
    bool graphComboFile = true;

    // TODO(luoli.hn) 临时方案,有增量的时候会有问题
    graphMaxDocCnt = docNum + 100;
    auto iterator = parameters.find(PARAM_GRAPH_COMMON_GRAPH_TYPE);
    if (iterator != parameters.end()) {
        graphType = iterator->second;
    }

    iterator = parameters.find(PARAM_GRAPH_COMMON_WITH_QUANTIZER);
    if (iterator != parameters.end() && iterator->second == "true") {
        graphWithQuantizer = true;
    }

    iterator = parameters.find(PARAM_GRAPH_COMMON_COMBO_FILE);
    if (iterator != parameters.end() && iterator->second == "false") {
        graphComboFile = false;
    }
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, &graphMaxScanNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, &graphNeighborCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, &memoryQuota);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION, &hnswEfconstruction);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, &hnswMaxLevel);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, &hnswScalingFactor);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, &hnswUpperNeighborCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_SEARCHER_EF, &hnswEf);

    // 检查以及纠正参数
    if (graphMaxScanNum > (uint64_t)docNum) {
        IE_LOG(DEBUG, "params[%s]'s value[%lu] > doc size[%d], use[%d] as its value",
               proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM.c_str(), graphMaxScanNum, docNum, docNum);
        graphMaxScanNum = docNum;
    }
    if (graphNeighborCnt > (uint64_t)docNum) {
        IE_LOG(DEBUG, "params[%s]'s value[%lu] > doc size[%d], use[%d] as its value",
               proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT.c_str(), graphNeighborCnt, docNum, docNum);
        graphNeighborCnt = docNum;
    }
    if (hnswEf > (uint64_t)docNum) {
        IE_LOG(DEBUG, "params[%s]'s value[%lu] > doc size[%d], use[%d] as its value",
               proxima::PARAM_HNSW_SEARCHER_EF.c_str(), hnswEf, docNum, docNum);
        hnswEf = docNum;
    }
    if (hnswEfconstruction > (uint64_t)docNum) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > doc size[%d], use[%d] as its value",
               proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION.c_str(), hnswEfconstruction, docNum, docNum);
        hnswEfconstruction = docNum;
    }
    if (hnswUpperNeighborCnt > graphNeighborCnt) {
        IE_LOG(DEBUG, "params[%s]'s value[%lu] > params[%s]'s value[%lu], use[%lu] as its value",
               proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT.c_str(), hnswUpperNeighborCnt,
               proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT.c_str(), graphNeighborCnt, graphNeighborCnt);
        hnswUpperNeighborCnt = graphNeighborCnt;
    }

    // 设置index parama参数
    indexParams.set(proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT, graphMaxDocCnt);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, graphType);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, graphMaxScanNum);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT, graphNeighborCnt);
    indexParams.set(proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA, memoryQuota);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER, graphWithQuantizer);
    indexParams.set(proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION, hnswEfconstruction);
    indexParams.set(proxima::PARAM_HNSW_BUILDER_MAX_LEVEL, hnswMaxLevel);
    indexParams.set(proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR, hnswScalingFactor);
    indexParams.set(proxima::PARAM_HNSW_BUILDER_UPPER_NEIGHBOR_CNT, hnswUpperNeighborCnt);
    indexParams.set(proxima::PARAM_HNSW_SEARCHER_EF, hnswEf);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_COMBO_FILE, graphComboFile);

    if (printParam) {
        IE_LOG(DEBUG,
               "graph build param init:[%s = %lu], [%s = %s], [%s = %lu], [%s = %lu], [%s = %lu], [%s = %d], [%s "
               "= %lu], [%s = %lu], [%s = %lu], [%s = %lu], [%s = %d]",
               proxima::PARAM_GRAPH_COMMON_MAX_DOC_CNT.data(), graphMaxDocCnt,
               proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE.data(), graphType.c_str(),
               proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM.data(), graphMaxScanNum,
               proxima::PARAM_GRAPH_COMMON_NEIGHBOR_CNT.data(), graphNeighborCnt,
               proxima::PARAM_GRAPH_BUILDER_MEMORY_QUOTA.data(), memoryQuota,
               proxima::PARAM_GRAPH_COMMON_WITH_QUANTIZER.data(), graphWithQuantizer,
               proxima::PARAM_HNSW_BUILDER_EFCONSTRUCTION.data(), hnswEfconstruction,
               proxima::PARAM_HNSW_BUILDER_MAX_LEVEL.data(), hnswMaxLevel,
               proxima::PARAM_HNSW_BUILDER_SCALING_FACTOR.data(), hnswScalingFactor,
               proxima::PARAM_HNSW_SEARCHER_EF.data(), hnswEf, proxima::PARAM_GRAPH_COMMON_COMBO_FILE.data(),
               graphComboFile);
    }

    return true;
}

bool GraphKnnParamsSelector::InitSearcherParams(const indexlib::util::KeyValueMap &parameters,
                                                aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docNum = 0;
    ParamUtil::ExtractValue(parameters, DOC_NUM, &docNum);
    indexlib::util::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docNum, dynamicParams)) {
        IE_LOG(DEBUG, "donnot need use dynamic params");
    }

    ParamUtil::MergeParams(parameters, dynamicParams, true);
    string graphType = "hnsw";
    uint64_t hnswEf = 100;
    uint64_t graphMaxScanNum = 15000;
    bool graphComboFile = true;

    auto iterator = parameters.find(PARAM_GRAPH_COMMON_GRAPH_TYPE);
    if (iterator != parameters.end()) {
        graphType = iterator->second;
    }

    iterator = parameters.find(PARAM_GRAPH_COMMON_COMBO_FILE);
    if (iterator != parameters.end() && iterator->second == "false") {
        graphComboFile = false;
    }

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_SEARCHER_EF, &hnswEf);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, &graphMaxScanNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GRAPH_COMMON_COMBO_FILE, &graphComboFile);
    // 检查以及纠正参数
    if (graphMaxScanNum > (uint64_t)docNum) {
        IE_LOG(DEBUG, "params[%s]'s value[%ld] > doc size[%d], use[%d] as its value",
               proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM.c_str(), graphMaxScanNum, docNum, docNum);
        graphMaxScanNum = docNum;
    }

    if (hnswEf > (uint64_t)docNum) {
        IE_LOG(DEBUG, "params[%s]'s value[%lu] > doc size[%d], use[%d] as its value",
               proxima::PARAM_HNSW_SEARCHER_EF.c_str(), hnswEf, docNum, docNum);
        hnswEf = docNum;
    }

    // 设置index parama参数
    indexParams.set(proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, graphType);
    indexParams.set(proxima::PARAM_HNSW_SEARCHER_EF, hnswEf);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM, graphMaxScanNum);
    indexParams.set(proxima::PARAM_GRAPH_COMMON_COMBO_FILE, graphComboFile);

    if (printParam) {
        IE_LOG(DEBUG, "graph build param init:[%s = %s], [%s = %lu], [%s = %lu]",
               proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE.data(), graphType.c_str(), proxima::PARAM_HNSW_SEARCHER_EF.data(),
               hnswEf, proxima::PARAM_GRAPH_COMMON_MAX_SCAN_NUM.data(), graphMaxScanNum);
    }

    return true;
}

bool GraphKnnParamsSelector::InitStreamerParams(const indexlib::util::KeyValueMap &parameters,
                                                aitheta::IndexParams &indexParams, bool printParam) {
    float scanRatio;
    if (ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_MAX_SCAN_RATIO, &scanRatio))
        indexParams.set(proxima::PARAM_HNSW_STREAMER_MAX_SCAN_RATIO, scanRatio);

    uint64_t memQuota;
    if (ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_MEMORY_QUOTA, &memQuota))
        indexParams.set(proxima::PARAM_HNSW_STREAMER_MEMORY_QUOTA, memQuota);

    uint32_t bruteThreshold;
    if (ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_BRUTE_FORCE_THRESHOLD, &bruteThreshold))
        indexParams.set(proxima::PARAM_HNSW_STREAMER_BRUTE_FORCE_THRESHOLD, bruteThreshold);

    uint32_t threadCount = 1;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_GENERAL_STREAMER_THREAD_COUNT, &threadCount);
    indexParams.set(proxima::PARAM_GENERAL_STREAMER_THREAD_COUNT, threadCount);

    bool needMemoryOpt = true;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_GET_VECTOR_DISABLE, &needMemoryOpt);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_GET_VECTOR_DISABLE, needMemoryOpt);
    return true;
}

bool GraphKnnParamsSelector::EnableMipsParams(const indexlib::util::KeyValueMap &parameters) {
    bool ret = (indexlib::util::GetValueFromKeyValueMap(parameters, SEARCH_METRIC_TYPE) == INNER_PRODUCT);
    if (ret) {
        if (indexlib::util::GetValueFromKeyValueMap(parameters, MIPS_ENABLE) == "false") {
            ret = false;
        } else {
            IE_LOG(WARN, "Mips is forcely enabled in graph algo, although not set in params");
        }
    }
    return ret;
}

}
}
