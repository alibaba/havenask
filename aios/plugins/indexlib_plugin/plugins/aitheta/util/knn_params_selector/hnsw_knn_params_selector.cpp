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
#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/hnsw_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/param_util.h"
#include <aitheta/algorithm/mips_reformer.h>

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, HnswKnnParamsSelector);

bool HnswKnnParamsSelector::InitSearcherParams(const indexlib::util::KeyValueMap &parameters,
                                               aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docNum = 0;
    ParamUtil::ExtractValue(parameters, DOC_NUM, &docNum);
    indexlib::util::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docNum, dynamicParams)) {
        IE_LOG(DEBUG, "do not use dynamic params");
    }

    ParamUtil::MergeParams(parameters, dynamicParams, true);
    string graphType = "hnsw";
    uint64_t hnswEf = 100u;
    uint64_t maxScanCnt = 4096u;

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_SEARCHER_EF, &hnswEf);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HNSW_SEARCHER_MAX_SCAN_CNT, &maxScanCnt);

    // 设置index parama参数
    indexParams.set(proxima::PARAM_GRAPH_COMMON_GRAPH_TYPE, graphType);
    indexParams.set(proxima::PARAM_HNSW_SEARCHER_EF, hnswEf);
    indexParams.set(proxima::PARAM_HNSW_SEARCHER_MAX_SCAN_CNT, maxScanCnt);

    IE_LOG(DEBUG, "hnsw searcher params[%s]", indexParams.debugString().c_str());
    return true;
}

bool HnswKnnParamsSelector::InitStreamerParams(const indexlib::util::KeyValueMap &parameters,
                                               aitheta::IndexParams &indexParams, bool printParam) {
    float scanRatio = 0.1f;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_MAX_SCAN_RATIO, &scanRatio);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_MAX_SCAN_RATIO, scanRatio);

    uint32_t minScanNum = 4096u;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_MIN_SCAN_LIMIT, &minScanNum);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_MIN_SCAN_LIMIT, minScanNum);

    uint32_t maxScanNum = UINT32_MAX;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_MAX_SCAN_LIMIT, &maxScanNum);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_MAX_SCAN_LIMIT, maxScanNum);

    uint32_t neighborCnt = 100u;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_NEIGHBOR_CNT, &neighborCnt);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_NEIGHBOR_CNT, neighborCnt);

    uint32_t upperNeighborCnt = 50u;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_UPPER_NEIGHBOR_CNT, &upperNeighborCnt);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_UPPER_NEIGHBOR_CNT, upperNeighborCnt);

    uint32_t ef = 500u;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_EF, &ef);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_EF, ef);

    uint32_t efConstruction = 500u;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_EFCONSTRUCTION, &efConstruction);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_EFCONSTRUCTION, efConstruction);

    uint64_t memQuota = UINT64_MAX;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_MEMORY_QUOTA, &memQuota);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_MEMORY_QUOTA, memQuota);

    uint32_t bruteThreshold = 4096u;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_BRUTE_FORCE_THRESHOLD, &bruteThreshold);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_BRUTE_FORCE_THRESHOLD, bruteThreshold);

    uint32_t chunkSize = 524288u;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_CHUNK_SIZE, &chunkSize);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_CHUNK_SIZE, chunkSize);

    uint32_t threadCount = 1;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_GENERAL_STREAMER_THREAD_COUNT, &threadCount);
    indexParams.set(proxima::PARAM_GENERAL_STREAMER_THREAD_COUNT, threadCount);

    bool needMemoryOpt = true;
    ParamUtil::ExtractValue(parameters, proxima::PARAM_HNSW_STREAMER_GET_VECTOR_DISABLE, &needMemoryOpt);
    indexParams.set(proxima::PARAM_HNSW_STREAMER_GET_VECTOR_DISABLE, needMemoryOpt);

    IE_LOG(DEBUG, "hnsw streamer params[%s]", indexParams.debugString().c_str());
    return true;
}

}
}
