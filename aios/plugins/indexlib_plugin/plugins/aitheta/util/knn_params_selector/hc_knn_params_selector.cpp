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

#include "indexlib_plugin/plugins/aitheta/util/knn_params_selector/hc_knn_params_selector.h"
#include "indexlib_plugin/plugins/aitheta/util/param_util.h"
#include "indexlib_plugin/plugins/aitheta/common_define.h"

using namespace std;
using namespace autil;
using namespace autil::legacy;
using namespace aitheta;
using namespace proxima;
namespace indexlib {
namespace aitheta_plugin {

IE_LOG_SETUP(aitheta_plugin, HCKnnParamsSelector);
HCKnnParamsSelector::HCKnnParamsSelector(const KnnStrategies &strategies) : NoneLinearKnnParamsSelector(strategies) {
    mSearcherName = KNN_HC_SEARCHER;
}
HCKnnParamsSelector::~HCKnnParamsSelector() {}

bool HCKnnParamsSelector::InitBuilderParams(const indexlib::util::KeyValueMap &parameters,
                                            aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docNum = 0;
    ParamUtil::ExtractValue(parameters, DOC_NUM, &docNum);
    indexlib::util::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docNum, dynamicParams)) {
        IE_LOG(INFO, "donnot need use dynamic params");
    }

    // 将parameters 与 动态参数合并
    ParamUtil::MergeParams(parameters, dynamicParams, true);
    int32_t basicBlockSize = 16;
    int32_t centroidNumInLevel1 = 1000;
    int32_t centroidNumInLevel2 = 100;
    int32_t leafCentriodNum = 100000;
    int32_t levelCnt = 2;
    int32_t maxDocCnt = 50000000;
    int32_t trainSampleCnt = 0;
    int64_t memoryQuota = 128LL * 1024LL * 1024LL * 1024LL;
    float trainSampleRatio = 0.0f;

    string buildTempStorageDir = "hc_builder_temp";

    string builderCentroidsNum;
    if (ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROIDS_NUM, &builderCentroidsNum) &&
        !builderCentroidsNum.empty()) {
        indexParams.set(proxima::PARAM_HC_BUILDER_CENTROIDS_NUM, builderCentroidsNum);
    } else {
        ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1",
                                &centroidNumInLevel1);
        indexParams.set(proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1", centroidNumInLevel1);

        ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2",
                                &centroidNumInLevel2);
        indexParams.set(proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2", centroidNumInLevel2);

        ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_LEVEL_CNT, &levelCnt);
        indexParams.set(proxima::PARAM_HC_COMMON_LEVEL_CNT, levelCnt);

        leafCentriodNum = centroidNumInLevel1 * centroidNumInLevel2;
        indexParams.set(proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, leafCentriodNum);
    }

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, &basicBlockSize);
    indexParams.set(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, basicBlockSize);

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_MAX_DOC_CNT, &maxDocCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, &trainSampleCnt);
    ParamUtil::ExtractValue(dynamicParams, PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO, &trainSampleRatio);

    // TODO: equals to docNum + 1, tmp solution ,当后续向量支持增量之后此处会有问题!!!
    maxDocCnt = docNum + 1;
    indexParams.set(proxima::PARAM_HC_COMMON_MAX_DOC_CNT, maxDocCnt);

    trainSampleCnt = std::max(static_cast<int32_t>(trainSampleRatio * docNum), trainSampleCnt);
    if (!trainSampleCnt || trainSampleCnt > docNum) {
        trainSampleCnt = docNum;
    }
    indexParams.set(proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT, trainSampleCnt);

    auto iterator = parameters.find(TMP_DUMP_DIR);
    if (parameters.end() != iterator) {
        buildTempStorageDir = iterator->second;
    } else {
        IE_LOG(WARN, "build temp storage dir is not defined");
    }
    indexParams.set(proxima::PARAM_HC_BUILDER_BASIC_PATH, buildTempStorageDir);

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, &memoryQuota);
    indexParams.set(proxima::PARAM_HC_BUILDER_MEMORY_QUOTA, memoryQuota);

    indexParams.set(proxima::PARAM_HC_COMMON_COMBO_FILE, true);
    indexParams.set(proxima::PARAM_HC_BUILDER_CLUSTER_AUTO_TUNING, true);

    IE_LOG(DEBUG,
           "hc builder param init: [%s = %d], [%s = %d], [%s] = %d], [%s = %d], [%s = %d], [%s = %d], [%s = %d], "
           "[%s = %f], [%s = %ld], [%s = %d], [%s] = [%s]",
           proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE.data(), basicBlockSize,
           (proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1").data(), centroidNumInLevel1,
           (proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2").data(), centroidNumInLevel2,
           proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM.data(), leafCentriodNum,
           proxima::PARAM_HC_COMMON_LEVEL_CNT.data(), levelCnt, proxima::PARAM_HC_COMMON_MAX_DOC_CNT.data(), maxDocCnt,
           proxima::PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_COUNT.data(), trainSampleCnt,
           PARAM_GENERAL_BUILDER_TRAIN_SAMPLE_RATIO.data(), trainSampleRatio,
           proxima::PARAM_HC_BUILDER_MEMORY_QUOTA.data(), memoryQuota, DOC_NUM.data(), docNum, TMP_DUMP_DIR.data(),
           buildTempStorageDir.data());

    return true;
}

bool HCKnnParamsSelector::InitSearcherParams(const indexlib::util::KeyValueMap &parameters,
                                             aitheta::IndexParams &indexParams, bool printParam) {
    int32_t docNum = 0;
    ParamUtil::ExtractValue(parameters, DOC_NUM, &docNum);
    indexlib::util::KeyValueMap dynamicParams;
    if (!GetDynamicParams(parameters, docNum, dynamicParams)) {
        IE_LOG(INFO, "do not need use dynamic params");
    }

    if (EnableGpuSearch(SchemaParameter(parameters), docNum)) {
        mSearcherName = GPU_KNN_HC_SEARCHER;
    }

    ParamUtil::MergeParams(parameters, dynamicParams, true);

    int32_t basicBlockSize = 16;
    int32_t centroidNumInLevel1 = 1000;
    int32_t centroidNumInLevel2 = 100;
    int32_t leafCentriodNum = 100000;
    int32_t levelCnt = 2;
    int32_t maxDocCnt = 10000000;
    int32_t scanNumInLevel1 = 60;
    int32_t scanNumInLevel2 = 6000;
    int32_t maxScanNum = 50000;
    int32_t topK = 500;
    int32_t gpuQueryBatchSize = 9;
    int32_t gpuStreamCnt = 5;
    IndexDistance::Methods searchMetricType = GetMethodType(parameters, SEARCH_METRIC_TYPE);
    bool enableGpuComputeContriod = false;
    bool enableFp16Quantization = false;

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, &basicBlockSize);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "1",
                            &centroidNumInLevel1);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_BUILDER_CENTROID_NUM_IN_LEVEL_PREFIX + "2",
                            &centroidNumInLevel2);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_LEVEL_CNT, &levelCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_COMMON_MAX_DOC_CNT, &maxDocCnt);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1", &scanNumInLevel1);

    if (!ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2",
                                 &scanNumInLevel2)) {
        scanNumInLevel2 = scanNumInLevel1 * centroidNumInLevel2;
    }
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM, &maxScanNum);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_TOPK, &topK);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_MAX_QUERY_BATCH_SIZE, &gpuQueryBatchSize);
    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_GPU_STREAM_CNT, &gpuStreamCnt);

    auto iterator = parameters.find(SEARCH_METRIC_TYPE);
    if (iterator != parameters.end()) {
        if (iterator->second == L2) {
            searchMetricType = IndexDistance::kMethodFloatSquaredEuclidean;
        } else if (iterator->second != INNER_PRODUCT) {
            IE_LOG(WARN, "unknown metric type [%s]", iterator->second.data());
        }
    }

    indexParams.set(proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE, basicBlockSize);

    // TODO(zongyuan.wuzy): proxima 1.3+ will not need set these params
    leafCentriodNum = centroidNumInLevel1 * centroidNumInLevel2;
    indexParams.set(proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM, leafCentriodNum);
    indexParams.set(proxima::PARAM_HC_COMMON_LEVEL_CNT, levelCnt);
    maxDocCnt = docNum + 1;
    indexParams.set(proxima::PARAM_HC_COMMON_MAX_DOC_CNT, maxDocCnt);
    // TODO(zongyuan.wuzy): end

    indexParams.set(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1", scanNumInLevel1);
    indexParams.set(proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2", scanNumInLevel2);
    indexParams.set(proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM, maxScanNum);
    indexParams.set(proxima::PARAM_HC_SEARCHER_TOPK, topK);
    indexParams.erase(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD);
    indexParams.set(proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD, searchMetricType);
    indexParams.set(proxima::PARAM_HC_COMMON_COMBO_FILE, true);
    indexParams.set(proxima::PARAM_HC_SEARCHER_MAX_QUERY_BATCH_SIZE, gpuQueryBatchSize);
    indexParams.set(proxima::PARAM_HC_SEARCHER_GPU_STREAM_CNT, gpuStreamCnt);

    ParamUtil::ExtractValue(dynamicParams, proxima::PARAM_HC_SEARCHER_GPU_COMPUTE_CENTROID_ENABLE,
                            &enableGpuComputeContriod);
    if (enableGpuComputeContriod) {
        indexParams.set(proxima::PARAM_HC_SEARCHER_GPU_COMPUTE_CENTROID_ENABLE, enableGpuComputeContriod);
    }

    ParamUtil::ExtractValue(dynamicParams, FP16_QUANTIZATION_ENABLE, &enableFp16Quantization);
    if (enableFp16Quantization) {
        aitheta::IndexMeta::FeatureTypes featureType = aitheta::IndexMeta::FeatureTypes::kTypeFloat16;
        indexParams.set(proxima::PARAM_HC_BUILDER_FEATURE_TARGET_TYPE, featureType);
    }

    IE_LOG(DEBUG,
           "hc searcher param init: [%s = %d], [%s = %d], [%s] = %d], [%s = "
           "%d], [%s = %d], [%s = %d], [%s = %d], [%s] = %d], [%s = %d],"
           "[%s = %d], [%s = %d]",
           proxima::PARAM_HC_COMMON_BASIC_BLOCK_SIZE.data(), basicBlockSize,
           proxima::PARAM_HC_COMMON_LEAF_CENTROID_NUM.data(), leafCentriodNum,
           proxima::PARAM_HC_COMMON_LEVEL_CNT.data(), levelCnt, proxima::PARAM_HC_SEARCHER_MAX_SCAN_NUM.data(),
           maxScanNum, (proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "1").data(), scanNumInLevel1,
           (proxima::PARAM_HC_SEARCHER_SCAN_NUM_IN_LEVEL_PREFIX + "2").data(), scanNumInLevel2,
           proxima::PARAM_HC_COMMON_MAX_DOC_CNT.data(), maxDocCnt, proxima::PARAM_HC_SEARCHER_TOPK.data(), topK,
           proxima::PARAM_GENERAL_SEARCHER_SEARCH_METHOD.data(), searchMetricType,
           proxima::PARAM_HC_SEARCHER_MAX_QUERY_BATCH_SIZE.data(), gpuQueryBatchSize,
           proxima::PARAM_HC_SEARCHER_GPU_STREAM_CNT.data(), gpuStreamCnt);

    return true;
}

}
}
